
import logging
import hashlib
import hmac
import threading
import time
from flask_login import login_required, LoginManager, UserMixin, login_user

from flask import Flask, jsonify, redirect, render_template, request, session, url_for

from config import load_settings
from database import DatabaseRepository
from services.billing_service import BillingService
from services.google_sheets_service import GoogleSheetsClient
from services.mercadopago_service import MercadoPagoClient
from services.zapi_service import ZApiClient

# --- Flask-Login: User e user_loader globais ---
login_manager = LoginManager()

class User(UserMixin):
    def __init__(self, id):
        self.id = id

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
_scheduler_lock = threading.Lock()
_scheduler_started = False



def create_app() -> Flask:
    app = Flask(__name__)
    login_manager.init_app(app)
    # login_manager.login_view = 'login'  # Opcional

    # Configurações seguras para sessão em produção
    app.config['SESSION_COOKIE_SECURE'] = True
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

    # Rota para favicon.ico
    @app.route('/favicon.ico')
    def favicon():
        from flask import send_from_directory
        return send_from_directory('static', 'favicon.ico', mimetype='image/vnd.microsoft.icon')

    settings = load_settings()

    repo = DatabaseRepository(database_url=settings.database_url)
    repo.init_schema()

    zapi_client = ZApiClient(
        instance_id=settings.zapi_instance_id,
        token=settings.zapi_token,
        client_token=settings.zapi_client_token,
    )
    mp_client = MercadoPagoClient(
        access_token=settings.mp_access_token,
        app_base_url=settings.app_base_url,
    )
    sheets_client = GoogleSheetsClient(
        spreadsheet_id=settings.google_spreadsheet_id,
        worksheet_name=settings.google_worksheet_name or "Pagina1",
        service_account_file=settings.google_service_account_file,
    )
    billing_service = BillingService(
        settings=settings,
        repo=repo,
        zapi_client=zapi_client,
        mp_client=mp_client,
        sheets_client=sheets_client,
    )
    # ...existing code...

    @app.route("/api/enviar-link-pagamento", methods=["POST"])
    @login_required
    def enviar_link_pagamento():
        body = request.get_json(silent=True) or {}
        numero = str(body.get("numero", "")).strip()
        nome = str(body.get("nome", "Cliente")).strip()
        if not numero:
            return jsonify({"erro": "Informe o número do cliente"}), 400
        try:
            result = billing_service.send_payment_link_no_due(numero, nome)
            if not result or not result.get("ok"):
                return jsonify({
                    "erro": result.get("message", "Falha ao enviar link")
                }), result.get("status_code", 500)
            return jsonify({
                "status": "ok",
                "message": result.get("message", "Enviado com sucesso")
            }), 200
        except Exception as e:
            print(f"Erro no endpoint enviar-link-pagamento: {e}")
            return jsonify({"erro": f"Erro interno: {str(e)}"}), 500
    app.secret_key = settings.flask_secret_key

    @app.after_request
    def disable_api_cache(response):
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    @app.route("/", methods=["GET"])
    def home():
        return "Bot de Cobranca Online - v7"

    @app.route("/test", methods=["GET"])
    def test():
        return jsonify({"message": "Flask está funcionando corretamente"}), 200

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"}), 200

    @app.route("/debug-planilha", methods=["GET"])
    def debug_planilha():
        return jsonify({
            "status": "desativado",
            "mensagem": "Modo somente banco de dados ativo.",
        }), 200

    @app.route("/debug-db", methods=["GET"])
    def debug_db():
        """Diagnóstico de conexão com o banco de dados - endpoint público para debug"""
        try:
            # Verifica se DATABASE_URL está configurado
            database_url_set = bool(settings.database_url)
            
            # Tenta conectar ao banco
            test_client = repo.get_panel_clients()
            
            return jsonify({
                "status": "ok",
                "database_configured": database_url_set,
                "database_type": "postgresql" if repo.is_postgres else "sqlite",
                "clients_count": len(test_client),
                "test_connection": "success"
            }), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "database_configured": bool(settings.database_url),
                "database_type": "postgresql" if repo.is_postgres else "sqlite",
                "error": str(e),
                "error_type": type(e).__name__
            }), 500


    @app.route("/login", methods=["GET", "POST"])
    def login():
        error = ""
        if request.method == "POST":
            username = str(request.form.get("username", "")).strip()
            password = str(request.form.get("password", "")).strip()



            if username == settings.admin_username and password == settings.admin_password:
                user = User(username)
                login_user(user)
                return redirect(url_for("dashboard"))

            error = "Usuario ou senha invalidos"

        return render_template("login.html", error=error)


    @app.route("/logout", methods=["GET"])
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/dashboard", methods=["GET"])
    @login_required
    def dashboard():
        return render_template("dashboard.html")

    @app.route("/api/clientes-status", methods=["GET"])
    @login_required
    def clients_status():
        result = billing_service.get_clients_status()
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify({"clientes": result["clientes"], "resumo": result["resumo"]}), 200

    @app.route("/api/clientes", methods=["POST"])
    @login_required
    def create_client():
        body = request.get_json(silent=True) or {}
        number = str(body.get("numero", "")).strip()
        name = str(body.get("nome", "Cliente")).strip()
        login = str(body.get("login", "")).strip()
        due_date = str(body.get("vencimento", "")).strip()

        result = billing_service.save_panel_client(number=number, name=name, due_date=due_date, login=login)
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify({"status": "ok"}), 200

    @app.route("/api/clientes/<number>", methods=["PUT"])
    @login_required
    def update_client(number: str):
        body = request.get_json(silent=True) or {}
        body_number = str(body.get("numero", number)).strip()
        name = str(body.get("nome", "Cliente")).strip()
        login = str(body.get("login", "")).strip()
        due_date = str(body.get("vencimento", "")).strip()

        result = billing_service.update_panel_client(
            original_number=number,
            new_number=body_number,
            name=name,
            due_date=due_date,
            login=login,
        )
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify({"status": "ok"}), 200

    @app.route("/api/clientes/<number>", methods=["DELETE"])
    @login_required
    def delete_client(number: str):
        result = billing_service.remove_client(number)
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify({"status": "ok"}), 200

    @app.route("/api/sincronizar-pagamentos", methods=["POST"])
    @login_required
    def sync_payments():
        body = request.get_json(silent=True) or {}
        limit = body.get("limit", 100)

        try:
            limit = int(limit)
        except (TypeError, ValueError):
            return jsonify({"erro": "Limite invalido"}), 400

        result = billing_service.sync_approved_payments(limit=limit)
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify(result), 200

    @app.route("/api/importar-planilha", methods=["POST"])
    @login_required
    def import_sheet_clients():
        result = billing_service.import_sheet_clients()
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify(result), 200

    @app.route("/api/clientes-atrasados", methods=["GET"])
    @login_required
    def overdue_clients():
        result = billing_service.get_overdue_clients()
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify({"clientes": result["clientes"]}), 200

    @app.route("/api/config-disparos", methods=["GET"])
    @login_required
    def get_dispatch_config():
        result = billing_service.get_dispatch_settings()
        return jsonify(result), 200

    @app.route("/api/config-disparos", methods=["PUT"])
    @login_required
    def save_dispatch_config():
        body = request.get_json(silent=True) or {}
        enabled = bool(body.get("habilitado", False))
        time_1 = str(body.get("horario_1", "08:00")).strip()
        time_2 = str(body.get("horario_2", "12:00")).strip()
        time_3 = str(body.get("horario_3", "18:00")).strip()

        result = billing_service.save_dispatch_settings(enabled, time_1, time_2, time_3)
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify({"status": "ok"}), 200

    @app.route("/api/disparos/execucoes", methods=["GET"])
    @login_required
    def get_dispatch_runs():
        result = billing_service.get_recent_dispatch_executions()
        return jsonify(result), 200

    @app.route("/api/disparos/manual", methods=["POST"])
    @login_required
    def manual_dispatch():
        body = request.get_json(silent=True) or {}
        numbers = body.get("numeros") or []
        early_payment = bool(body.get("early_payment", False))
        if not isinstance(numbers, list):
            return jsonify({"erro": "Numeros invalidos"}), 400

        result = billing_service.send_manual_charges(numbers=numbers, early_payment=early_payment)
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]
        return jsonify(result), 200

    @app.route("/api/cobrancas", methods=["GET"])
    @login_required
    def get_charges():
        charges = repo.get_all_charges()
        return jsonify({"cobrancas": charges}), 200

    @app.route("/api/clientes/<number>/status-pagamento", methods=["POST"])
    @login_required
    def update_client_payment_status(number: str):
        body = request.get_json(silent=True) or {}
        numero_atual = str(body.get("numero_atual", number)).strip()
        status = str(body.get("status", "")).strip()

        result = billing_service.set_manual_payment_status(
            numero_original=number,
            numero_atual=numero_atual,
            status=status,
        )
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]

        # Mensagem de confirmação já é enviada por set_manual_payment_status quando status == 'approved'.

        return jsonify({"status": "ok", "pagamento": result["status"]}), 200

    @app.route("/api/teste-pagamento-aprovado", methods=["POST"])
    @login_required
    def test_approved_payment():
        body = request.get_json(silent=True) or {}
        number = str(body.get("numero", "")).strip()
        name = str(body.get("nome", "Cliente")).strip()
        amount = body.get("valor", settings.default_charge_amount)
        external_reference = str(body.get("external_reference", "teste-aprovado-manual")).strip()
        payment_id = str(body.get("payment_id", f"test-{external_reference}")).strip()
        send_whatsapp = bool(body.get("enviar_whatsapp", True))

        if not number:
            return jsonify({"erro": "Informe o numero"}), 400

        try:
            amount = float(amount)
        except (TypeError, ValueError):
            return jsonify({"erro": "Valor invalido"}), 400

        repo.upsert_charge(
            external_reference=external_reference,
            number=number,
            name=name,
            amount=amount,
            status="approved",
            payment_link="teste-manual",
        )
        repo.save_payment(payment_id, external_reference, number, "approved", amount)
        repo.update_charge_status(external_reference, "approved", payment_id)

        update_result = billing_service.update_due_date_after_approved_payment(number)
        if not update_result["ok"]:
            return jsonify({
                "erro": update_result["message"],
                "payment_id": payment_id,
                "external_reference": external_reference,
            }), 400

        whatsapp_status = None
        if send_whatsapp:
            whatsapp_status = zapi_client.send_text(
                number,
                "Pagamento aprovado com sucesso. Obrigado! Seu acesso sera renovado em instantes.",
            )

        return jsonify(
            {
                "status": "ok",
                "payment_id": payment_id,
                "external_reference": external_reference,
                "numero": number,
                "novo_vencimento": update_result["new_due_date"],
                "updated_remote": update_result["updated_remote"],
                "whatsapp_status": whatsapp_status,
            }
        ), 200

    @app.route("/webhook", methods=["POST"])
    def zapi_webhook():
        payload = request.get_json(silent=True) or {}
        if payload.get("fromMe") is False:
            number = str(payload.get("phone", "")).strip()
            repo.register_interaction(number)
        return jsonify({"status": "ok"}), 200

    @app.route("/mercadopago/criar-pagamento", methods=["POST"])
    def create_payment():
        body = request.get_json(silent=True) or {}
        amount = body.get("valor")
        number = str(body.get("numero", "")).strip()
        name = str(body.get("nome", "Cliente")).strip()
        description = str(body.get("descricao", "Renovacao de plano")).strip()

        if amount is None or not number:
            return jsonify({"erro": "Informe valor e numero"}), 400

        try:
            amount = float(amount)
        except (TypeError, ValueError):
            return jsonify({"erro": "Valor invalido"}), 400

        result = billing_service.create_payment(
            amount=amount,
            number=number,
            name=name,
            description=description,
        )
        if not result["ok"]:
            return jsonify({"erro": result["message"]}), result["status_code"]

        return (
            jsonify(
                {
                    "status": "ok",
                    "payment_link": result["payment_link"],
                    "external_reference": result["external_reference"],
                }
            ),
            200,
        )

    @app.route("/webhook/mercadopago", methods=["POST", "GET"])

    def mercadopago_webhook():
        import threading
        payload = request.get_json(silent=True) or {}
        args = request.args.to_dict()
        headers = dict(request.headers)
        logger.info("MP webhook recebido | args=%s | payload=%s", args, payload)

        def process_webhook(payload, args, headers):
            try:
                if settings.mp_webhook_secret:
                    x_signature = headers.get("x-signature", "")
                    x_request_id = headers.get("x-request-id", "")
                    data_id = args.get("data.id", "")
                    ts, received_hash = "", ""
                    for part in x_signature.split(","):
                        k, _, v = part.strip().partition("=")
                        if k == "ts":
                            ts = v
                        elif k == "v1":
                            received_hash = v
                    if ts and received_hash:
                        manifest = f"id:{data_id};request-id:{x_request_id};ts:{ts};"
                        expected = hmac.new(settings.mp_webhook_secret.encode(), manifest.encode(), hashlib.sha256).hexdigest()
                        if not hmac.compare_digest(expected, received_hash):
                            logger.warning("MP webhook com assinatura invalida")
                            return

                # --- Tratar notificao order.processed (link fixo / QR Code MP) ---
                event_type = str(payload.get("type") or args.get("type") or "").strip().lower()
                if event_type.startswith("order"):
                    order_id = str((payload.get("data") or {}).get("id") or args.get("id") or "").strip()
                    if not order_id:
                        logger.info("MP webhook order ignorado | sem order_id | payload=%s", payload)
                        return

                    order = mp_client.get_merchant_order(order_id)
                    if not order:
                        logger.warning("Falha ao consultar merchant_order %s", order_id)
                        return

                    logger.info("MP merchant_order recebido | id=%s | status=%s", order_id, order.get("status"))

                    # Verificar se order esta paga (soma dos pagamentos approved >= total)
                    order_total = float(order.get("total_amount") or 0)
                    payments_in_order = [p for p in (order.get("payments") or []) if isinstance(p, dict)]
                    paid_total = sum(float(p.get("total_paid_amount") or 0) for p in payments_in_order if str(p.get("status", "")) == "approved")

                    order_paid = order.get("status") == "closed" or (order_total > 0 and paid_total >= order_total)

                    for mp_payment in payments_in_order:
                        p_id = str(mp_payment.get("id") or "").strip()
                        p_status = str(mp_payment.get("status") or "").strip()
                        p_amount = float(mp_payment.get("total_paid_amount") or 0)
                        ext_ref = str(order.get("external_reference") or "").strip()

                        if not p_id:
                            continue

                        # Buscar detalhes completos do pagamento para identificar o cliente
                        full_payment = mp_client.get_payment(p_id) or mp_payment
                        number = billing_service.extract_client_number_from_payment(full_payment)

                        repo.save_payment(p_id, ext_ref, number, p_status, p_amount)
                        if ext_ref:
                            repo.update_charge_status(ext_ref, p_status, p_id)

                        if p_status == "approved" and number and order_paid:
                            update_result = billing_service.update_due_date_after_approved_payment(number)
                            if not update_result.get("ok"):
                                logger.warning("Nao foi possivel atualizar vencimento de %s: %s", number, update_result.get("message"))

                            first_name = ""
                            try:
                                clients = billing_service._get_combined_clients()
                                for c in clients:
                                    if c.get("numero") == number:
                                        first_name = (c.get("nome") or "").split()[0]
                                        break
                            except Exception:
                                pass

                            nome = first_name or "cliente"
                            valor_pago = p_amount if 'p_amount' in locals() else amount if 'amount' in locals() else ''
                            valor_pago_str = f"{valor_pago:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor_pago else ""
                            vencimento = new_due or ""
                            payment_link = getattr(settings, 'payment_link', 'https://link.mercadopago.com.br/assinaturatvrodrigo')
                            mensagem = (
                                "🤖Mensagem automatica\n\n"
                                f"Olá, {nome} 👋\n\n"
                                f"📅 Seu plano vence em {vencimento}.\n\n"
                                "Segue os dados para pagamentos:\n"
                                "Nome: Rodrigo Batista dos Santos\n"
                                "Banco: Mercado Pago\n\n"
                                "Para renovar, utilize:\n"
                                f"🔗 Link de pagamento: {payment_link}\n\n"
                                "Caso ja tenha pago, Desconsidere a mensagem.\n"
                                "Qualquer dúvida, estamos à disposição! 😊"
                            )
                            zapi_client.send_text(number, mensagem)
                            logger.info("Pagamento order aprovado e processado | numero=%s | order=%s", number, order_id)
                    return

                # --- Tratar notificacao payment (checkout preference) ---
                payment_id = billing_service.extract_payment_notification(payload, args)
                if not payment_id:
                    logger.info("MP webhook ignorado | motivo=evento sem payment_id valido | args=%s | payload=%s", args, payload)
                    return

                payment = mp_client.get_payment(payment_id)
                if not payment:
                    logger.warning("Falha ao consultar pagamento %s", payment_id)
                    sync_result = billing_service.sync_approved_payments(limit=30)
                    if sync_result.get("ok"):
                        logger.info("Fallback de sincronizacao executado apos falha no webhook: %s", sync_result)
                    else:
                        logger.warning("Fallback de sincronizacao falhou: %s", sync_result.get("message"))
                    return

                status = str(payment.get("status", ""))
                external_reference = str(payment.get("external_reference", "")).strip()
                number = billing_service.extract_client_number_from_payment(payment)
                amount = float(payment.get("transaction_amount", 0) or 0)

                repo.save_payment(payment_id, external_reference, number, status, amount)
                if external_reference:
                    repo.update_charge_status(external_reference, status, payment_id)

                if status == "approved" and number:
                    update_result = billing_service.update_due_date_after_approved_payment(number)
                    if not update_result.get("ok"):
                        logger.warning("Nao foi possivel atualizar vencimento de %s: %s", number, update_result.get("message"))

                    first_name = ""
                    try:
                        clients = billing_service._get_combined_clients()
                        for c in clients:
                            if c.get("numero") == number:
                                first_name = (c.get("nome") or "").split()[0]
                                break
                    except Exception:
                        pass

                    nome = first_name or "cliente"
                    valor_pago = amount if 'amount' in locals() else ''
                    valor_pago_str = f"{valor_pago:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor_pago else ""
                    vencimento = new_due or ""
                    mensagem = (
                        f"Olá, *{nome}*! 🎉\n\n"
                        f"✅ Seu pagamento de R$ {valor_pago_str} foi confirmado com sucesso!\n\n"
                        "Muito obrigado por continuar com a gente. Seu acesso será liberado em instantes.\n\n"
                        f"📅 Novo vencimento: *{vencimento}*\n\n"
                        "Qualquer dúvida, estamos à disposição. 😊"
                    )
                    zapi_client.send_text(number, mensagem)
            except Exception as e:
                logger.error(f"Erro no processamento do webhook em background: {e}")
                # Aqui garantimos que qualquer erro não afete a resposta ao Mercado Pago
                pass

        # Responde imediatamente ao Mercado Pago
        threading.Thread(target=process_webhook, args=(payload, args, headers)).start()
        return jsonify({"status": "ok"}), 200

    @app.route("/enviar-cobrancas", methods=["GET"])
    def send_charges():
        result = billing_service.process_daily_charges()
        if not result["ok"]:
            return result["message"], result["status_code"]
        return result["message"], 200

    def scheduler_loop() -> None:
        logger.info("Scheduler interno de disparos iniciado")
        dispatch_supported = True
        while True:
            try:
                if not dispatch_supported:
                    # Keep thread alive without dispatch checks when running older configs.
                    time.sleep(30)
                    continue

                dispatch_settings = billing_service.get_dispatch_settings()
                if dispatch_settings.get("habilitado"):
                    now_br = billing_service._now_br().strftime("%H:%M")
                    for slot_name in ("horario_1", "horario_2", "horario_3"):
                        slot_time = str(dispatch_settings.get(slot_name, "")).strip()
                        if slot_time and slot_time == now_br:
                            result = billing_service.run_scheduled_dispatch(slot_name)
                            logger.info("Disparo automatico %s executado: %s", slot_name, result)
                time.sleep(30)
            except AttributeError as exc:
                if "get_dispatch_settings" in str(exc):
                    dispatch_supported = False
                    logger.warning(
                        "Configuracao de disparos indisponivel nesta versao. Scheduler automatico desativado."
                    )
                    time.sleep(30)
                    continue
                logger.exception("Falha no scheduler interno: %s", exc)
                time.sleep(30)
            except Exception as exc:
                logger.exception("Falha no scheduler interno: %s", exc)
                time.sleep(30)

    # Global error handler
    @app.errorhandler(500)
    def handle_500_error(error):
        logger.exception("Error 500: %s", error)
        return jsonify({
            "status": "error",
            "message": "Internal server error",
            "type": type(error).__name__,
            "details": str(error)
        }), 500

    @app.errorhandler(Exception)
    def handle_exception(error):
        logger.exception("Unhandled exception: %s", error)
        return jsonify({
            "status": "error",
            "message": "Unhandled exception",
            "type": type(error).__name__,
            "details": str(error)
        }), 500

    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            threading.Thread(target=scheduler_loop, daemon=True).start()
            _scheduler_started = True

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
