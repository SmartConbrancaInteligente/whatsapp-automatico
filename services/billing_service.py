from datetime import datetime, timedelta
import re
import time
from typing import Any, Dict, Optional

from config import Settings
from database import DatabaseRepository
from services.google_sheets_service import GoogleSheetsClient
from services.mercadopago_service import MercadoPagoClient
from services.zapi_service import ZApiClient



class BillingService:
    def send_payment_link_no_due(self, number: str, name: str = "Cliente") -> Dict[str, Any]:
        """Envia o link de pagamento sem data de vencimento, para pagamento antecipado."""
        first_name = name.split()[0] if name.split() else name
        fixed_payment_link = str(self.settings.payment_link or "https://link.mercadopago.com.br/assinaturatvrodrigo").strip()
        if not fixed_payment_link:
            return {"ok": False, "message": "Link de pagamento não configurado", "status_code": 400}
        mensagem = (
            f"Olá, {first_name}! 😊\n\n"
            "Aqui está seu link para pagamento antecipado:\n"
            f"🔗 {fixed_payment_link}\n\n"
            "Assim que o pagamento for aprovado, avisaremos por aqui. Qualquer dúvida, estamos à disposição!"
        )
        status_code = self.zapi_client.send_text(number, mensagem)
        return {"ok": status_code in (200, 201), "status_code": status_code, "message": "Enviado" if status_code in (200, 201) else "Falha ao enviar"}

    def __init__(
        self,
        settings: Settings,
        repo: DatabaseRepository,
        zapi_client: ZApiClient,
        mp_client: MercadoPagoClient,
        sheets_client: GoogleSheetsClient,
    ) -> None:
        self.settings = settings
        self.repo = repo
        self.zapi_client = zapi_client
        self.mp_client = mp_client
        self.sheets_client = sheets_client
        self._overrides_cache: Dict[str, str] = {}
        self._overrides_cache_expires_at: float = 0.0

    @staticmethod
    def _now_br() -> datetime:
        return datetime.utcnow() - timedelta(hours=3)

    @classmethod
    def _today_br(cls) -> str:
        return cls._now_br().strftime("%d/%m/%Y")

    @staticmethod
    def _parse_due_date(value: str) -> Optional[datetime]:
        try:
            parsed = datetime.strptime(str(value).strip(), "%d/%m/%Y")
        except ValueError:
            return None
        return parsed.replace(hour=0, minute=0, second=0, microsecond=0)

    def _get_number_overrides_cached(self) -> Dict[str, str]:
        now = time.time()
        if now < self._overrides_cache_expires_at:
            return self._overrides_cache

        get_overrides = getattr(self.repo, "get_number_overrides", None)
        if callable(get_overrides):
            self._overrides_cache = get_overrides()
        else:
            self._overrides_cache = {}
        self._overrides_cache_expires_at = time.time() + 60
        return self._overrides_cache

    def extract_client_number_from_payment(self, payment: Dict[str, Any]) -> str:
        """Try to identify the client phone number from an MP payment object.

        Strategies (in order):
        1. metadata.numero  — set when checkout preference was created by this app.
        2. external_reference  — look up in our DB which number generated this charge.
        3. Payer phone from PIX  — MP returns payer.phone for PIX payments; try to
           match against known client numbers stored in clientes_painel.
        """
        # 1. metadata
        metadata = payment.get("metadata") or {}
        number = str(metadata.get("numero", "")).strip()
        if number:
            return number

        # 2. external_reference
        external_reference = str(payment.get("external_reference", "")).strip()
        if external_reference:
            number = self.repo.get_number_by_external_reference(external_reference) or ""
            if number:
                return number

        # 3. payer phone (PIX direct payments)
        payer = payment.get("payer") or {}
        phone = payer.get("phone") or {}
        area_code = str(phone.get("area_code", "")).strip().lstrip("0")
        phone_number = str(phone.get("number", "")).strip().replace("-", "").replace(" ", "")

        if area_code and phone_number:
            panel_numbers = {str(c.get("numero", "")).strip() for c in self.repo.get_panel_clients()}
            candidate = f"55{area_code}{phone_number}"
            if candidate in panel_numbers:
                return candidate
            # Fallback: bare area_code+number (some stores omit country code)
            alt = f"{area_code}{phone_number}"
            for pn in panel_numbers:
                if pn.endswith(alt):
                    return pn

        return ""

    def _resolve_original_number(self, number: str, number_overrides: Optional[Dict[str, str]] = None) -> str:
        if not number:
            return ""

        overrides = number_overrides or self._get_number_overrides_cached()
        for original, current in overrides.items():
            if current == number:
                return original
        return number

    def _get_combined_clients(self) -> list[Dict[str, str]]:
        hidden_numbers = self.repo.get_hidden_client_numbers()
        number_overrides = self._get_number_overrides_cached()
        reverse_overrides = {current: original for original, current in number_overrides.items() if current}
        clients_by_number: Dict[str, Dict[str, str]] = {}

        for row in self.repo.get_panel_clients():
            number = str(row.get("numero", "")).strip()
            original_number = reverse_overrides.get(number, number)
            if not number or original_number in hidden_numbers or number in hidden_numbers:
                continue

            clients_by_number[number] = {
                "nome": str(row.get("nome", "Cliente")).strip(),
                "numero": number,
                "numero_original": original_number,
                "login": str(row.get("login", "")).strip(),
                "vencimento": str(row.get("vencimento", "")).strip(),
                "origem": "painel",
            }

        return sorted(clients_by_number.values(), key=lambda item: (item["nome"].lower(), item["numero"]))

    def set_manual_payment_status(
        self,
        numero_original: str,
        numero_atual: str,
        status: str,
        amount: Optional[float] = None,
    ) -> Dict[str, Any]:
        if status not in {"approved", "pending"}:
            return {"ok": False, "message": "Status invalido", "status_code": 400}

        current_number = str(numero_atual or numero_original).strip()
        number_overrides = self._get_number_overrides_cached()
        original_number = self._resolve_original_number(str(numero_original or current_number).strip(), number_overrides)
        if not current_number or not original_number:
            return {"ok": False, "message": "Numero nao informado", "status_code": 400}

        payment_amount = float(amount or self.settings.default_charge_amount)
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        payment_id = f"manual-{status}-{original_number}-{timestamp}"

        self.repo.save_payment(
            payment_id=payment_id,
            external_reference="",
            number=current_number,
            status=status,
            amount=payment_amount,
        )

        if status == "approved":
            update_result = self.update_due_date_after_approved_payment(original_number, current_number, number_overrides)
            # Enviar mensagem de confirmação de pagamento
            # Buscar nome do cliente
            first_name = "Cliente"
            try:
                clients = self._get_combined_clients()
                for c in clients:
                    if c.get("numero") == current_number:
                        first_name = (c.get("nome") or "").split()[0]
                        break
            except Exception:
                pass
            valor_pago = payment_amount
            valor_pago_str = f"{valor_pago:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor_pago else ""
            vencimento = update_result.get("new_due_date") or ""
            mensagem = (
                f"Olá, *{first_name}*! 🎉\n\n"
                f"✅ Seu pagamento foi confirmado com sucesso!\n\n"
                "Muito obrigado por continuar com a gente. Seu acesso será liberado em instantes.\n\n"
                f"📅 Novo vencimento: *{vencimento}*\n\n"
                "Qualquer dúvida, estamos à disposição. 😊"
            )
            self.zapi_client.send_text(current_number, mensagem)

        return {
            "ok": True,
            "status_code": 200,
            "status": status,
        }

    def _send_charge_to_client(self, number: str, name: str, due_date: str) -> bool:
        # Não envia cobrança se cliente estiver oculto (pausado)
        hidden_numbers = self.repo.get_hidden_client_numbers()
        if number in hidden_numbers:
            return False
        first_name = name.split()[0] if name.split() else name
        fixed_payment_link = str(self.settings.payment_link or "https://link.mercadopago.com.br/assinaturatvrodrigo").strip()

        if fixed_payment_link:
            link_to_send = fixed_payment_link
        else:
            latest_charge = self.repo.get_latest_charge_by_number(number)
            link_to_send = (
                latest_charge.get("payment_link", "")
                if latest_charge and latest_charge.get("status") == "pending"
                else ""
            )

        if link_to_send:
            message = (
                "🤖 MENSAGEM AUTOMÁTICA\n\n"
                f"Olá, {first_name}! 😊\n\n"
                f"Seu plano de TV vence hoje, {due_date}.\n"
                "Deseja renovar? 📺\n"
                "💳 Dados para renovação:\n"
                "👤 Rodrigo Batista dos Santos\n"
                "🏦 Banco Mercado Pago\n\n"
                "🔗 Link de pagamento:\n"
                f"{link_to_send}\n\n"
                "✅ Após a confirmação, seu plano será ativado automaticamente.\n\n"
                "⚠️ Caso já tenha efetuado o pagamento, por gentileza desconsidere esta mensagem."
            )
            status_code = self.zapi_client.send_text(number, message)
            return status_code in (200, 201)

     

    def get_overdue_clients(self) -> Dict[str, Any]:
        try:
            rows = list(self._get_combined_clients())
        except Exception as exc:
            return {"ok": False, "message": f"Erro ao acessar clientes: {exc}", "status_code": 500}

        latest_payment_map = self.repo.get_latest_payment_status_by_number()
        due_overrides = self.repo.get_due_date_overrides()
        today = self._now_br().replace(hour=0, minute=0, second=0, microsecond=0)
        overdue_clients = []

        for row in rows:
            number = str(row.get("numero", "")).strip()
            original_number = str(row.get("numero_original", number)).strip()
            name = str(row.get("nome", "Cliente")).strip()
            row_due_date = str(row.get("vencimento", "")).strip()
            if str(row.get("origem", "")).strip() == "painel":
                due_date = row_due_date
            else:
                due_date = due_overrides.get(original_number, row_due_date)
            due_date_parsed = self._parse_due_date(due_date)
            if not number or due_date_parsed is None:
                continue

            payment_status_raw = latest_payment_map.get(number, "pending")
            if payment_status_raw == "approved":
                continue

            if due_date_parsed > today:
                continue

            overdue_clients.append(
                {
                    "nome": name,
                    "numero": number,
                    "vencimento": due_date,
                    "origem": str(row.get("origem", "planilha")),
                    "dias_atraso": max((today - due_date_parsed).days, 0),
                    "status_pagamento_raw": payment_status_raw,
                }
            )

        overdue_clients.sort(key=lambda item: (-int(item["dias_atraso"]), item["nome"].lower(), item["numero"]))
        return {"ok": True, "status_code": 200, "clientes": overdue_clients}

    def send_manual_charges(self, numbers: Optional[list[str]] = None, early_payment: bool = False) -> Dict[str, Any]:
        # Não envia para clientes ocultos (pausados)
        hidden_numbers = self.repo.get_hidden_client_numbers()
        overdue_result = self.get_overdue_clients()
        if not overdue_result["ok"]:
            return overdue_result

        overdue_clients = overdue_result["clientes"]
        if numbers:
            target_numbers = {str(number).strip() for number in numbers if str(number).strip()}
            overdue_clients = [item for item in overdue_clients if item["numero"] in target_numbers]

        sent_count = 0
        skipped_interaction = 0
        for client in overdue_clients:
            number = client["numero"]
            nome = client["nome"]
            vencimento = client["vencimento"]
            if number in hidden_numbers:
                skipped_interaction += 1
                continue
            if self.repo.has_interacted_today(number):
                skipped_interaction += 1
                continue
            if early_payment:
                # Mensagem personalizada de pagamento adiantado
                result = self.send_payment_link_no_due(number, nome)
                if result.get("ok"):
                    sent_count += 1
            else:
                if self._send_charge_to_client(number, nome, vencimento):
                    sent_count += 1

        return {
            "ok": True,
            "status_code": 200,
            "enviados": sent_count,
            "ignorados_interacao": skipped_interaction,
            "total_alvo": len(overdue_clients),
        }

    def process_daily_charges(self) -> Dict[str, Any]:
        result = self.send_manual_charges()
        if not result["ok"]:
            return result
        return {
            "ok": True,
            "message": f"Sucesso! {result['enviados']} cobrancas enviadas.",
            "status_code": 200,
        }

    def create_payment(self, amount: float, number: str, name: str, description: str) -> Dict[str, Any]:
        preference, error = self.mp_client.create_checkout_preference(
            amount=amount,
            description=description,
            number=number,
        )
        if error or not preference:
            return {"ok": False, "message": error or "Falha ao criar pagamento", "status_code": 500}

        self.repo.upsert_charge(
            external_reference=str(preference["external_reference"]),
            number=number,
            name=name,
            amount=amount,
            status="pending",
            payment_link=str(preference["init_point"]),
        )

        charge_code = str(preference["external_reference"])[:8].upper()
        message = (
            f"Ola, {name}!\n"
            f"Aqui esta seu link de pagamento: {preference['init_point']}\n"
            f"Codigo da cobranca: {charge_code}\n"
            "Assim que o pagamento for aprovado, vou te confirmar automaticamente por aqui."
        )
        self.zapi_client.send_text(number, message)

        return {
            "ok": True,
            "status_code": 200,
            "payment_link": preference["init_point"],
            "external_reference": preference["external_reference"],
        }

    def update_due_date_after_approved_payment(
        self,
        number: str,
        current_number: Optional[str] = None,
        number_overrides: Optional[Dict[str, str]] = None,
        send_message: bool = True,
        amount: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not number:
            return {"ok": False, "message": "Numero nao informado"}

        overrides = number_overrides or self._get_number_overrides_cached()
        original_number = self._resolve_original_number(number, overrides)
        if current_number is None:
            current_number = original_number

        panel_client = self.repo.get_panel_clients()
        client_data = None
        for client in panel_client:
            if client.get("numero") == current_number or client.get("numero") == number:
                client_data = client
                break

        if client_data:
            current_due_date = str(client_data.get("vencimento", "")).strip()
            client_name = str(client_data.get("nome", "Cliente")).strip()
            client_login = str(client_data.get("login", "")).strip()
        else:
            due_overrides = self.repo.get_due_date_overrides()
            current_due_date = due_overrides.get(original_number, "").strip()
            client_name = "Cliente"
            client_login = ""

        if not current_due_date:
            return {"ok": True, "new_due_date": None, "updated_remote": False}

        new_due_date = GoogleSheetsClient.add_one_month(current_due_date)
        if not new_due_date:
            return {"ok": True, "new_due_date": None, "updated_remote": False}

        if client_data:
            self.repo.upsert_panel_client(current_number, client_name, new_due_date, client_login)
        self.repo.upsert_due_date_override(original_number, new_due_date)

        # Mensagem de confirmação removida conforme solicitado

        return {
            "ok": True,
            "new_due_date": new_due_date,
            "updated_remote": False,
        }

    def get_clients_status(self) -> Dict[str, Any]:
        try:
            rows = list(self._get_combined_clients())
        except Exception as exc:
            return {"ok": False, "message": f"Erro ao acessar clientes no banco: {exc}", "status_code": 500}

        latest_payment_map = self.repo.get_latest_payment_status_by_number()
        latest_payment_details = self.repo.get_latest_payment_details_by_number()
        due_overrides = self.repo.get_due_date_overrides()

        clients = []
        total_paid = 0.0
        total_pending = 0.0

        hidden_numbers = self.repo.get_hidden_client_numbers()
        for row in rows:
            number = str(row.get("numero", "")).strip()
            original_number = str(row.get("numero_original", number)).strip()
            name = str(row.get("nome", "Cliente")).strip()
            row_due_date = str(row.get("vencimento", "")).strip()
            if str(row.get("origem", "")).strip() == "painel":
                due_date = row_due_date
            else:
                due_date = due_overrides.get(original_number, row_due_date)

            payment_status_raw = str(latest_payment_map.get(number, "pending")).strip().lower()
            payment_status = "pago" if payment_status_raw == "approved" else "nao_pago"

            detail = latest_payment_details.get(number)
            if payment_status == "pago":
                amount_paid = detail["amount"] if detail else self.settings.default_charge_amount
                total_paid += float(amount_paid)
            else:
                total_pending += float(self.settings.default_charge_amount)

            clients.append(
                {
                    "nome": name,
                    "numero": number,
                    "numero_original": original_number,
                    "login": str(row.get("login", "")).strip(),
                    "vencimento": due_date,
                    "origem": str(row.get("origem", "painel")),
                    "status_pagamento": payment_status,
                    "pausado": number in hidden_numbers,
                }
            )

        return {
            "ok": True,
            "status_code": 200,
            "clientes": clients,
            "resumo": {
                "total_recebido": round(total_paid, 2),
                "total_pendente": round(total_pending, 2),
                "saldo_liquido": round(total_paid - total_pending, 2),
            },
        }

    @staticmethod
    def extract_payment_notification(payload: Dict[str, Any], args: Dict[str, Any]) -> Optional[str]:
        event_type_raw = str(payload.get("type") or payload.get("topic") or args.get("type") or args.get("topic") or "").strip().lower()
        payment_id: Optional[str] = None

        data = payload.get("data") or {}
        if isinstance(data, dict) and data.get("id") is not None:
            payment_id = str(data.get("id")).strip()

        if not payment_id:
            payment_id = str(payload.get("data.id") or args.get("data.id") or args.get("id") or "").strip()

        if not payment_id:
            resource = str(payload.get("resource") or args.get("resource") or "").strip()
            match = re.search(r"/v1/payments/(\d+)", resource)
            if match:
                payment_id = match.group(1)

        is_payment_event = (
            not event_type_raw
            or event_type_raw == "payment"
            or event_type_raw.startswith("payment.")
        )

        if not is_payment_event or not payment_id:
            return None

        return payment_id

    def save_panel_client(self, number: str, name: str, due_date: str, login: str = "") -> Dict[str, Any]:
        if not number:
            return {"ok": False, "message": "Numero nao informado", "status_code": 400}
        if not due_date:
            return {"ok": False, "message": "Vencimento nao informado", "status_code": 400}

        try:
            datetime.strptime(due_date.strip(), "%d/%m/%Y")
        except ValueError:
            return {"ok": False, "message": "Vencimento invalido. Use dd/mm/aaaa", "status_code": 400}

        target_number = number.strip()
        resolved_original = self._resolve_original_number(target_number)

        self.repo.upsert_panel_client(
            target_number,
            (name or "Cliente").strip(),
            due_date.strip(),
            str(login or "").strip(),
        )
        # Keep dashboard and overdue lists in sync when an old due-date override exists.
        self.repo.upsert_due_date_override(resolved_original, due_date.strip())
        self.repo.unhide_client(target_number)
        return {"ok": True, "status_code": 200}

    def update_panel_client(self, original_number: str, new_number: str, name: str, due_date: str, login: str = "") -> Dict[str, Any]:
        if not original_number:
            return {"ok": False, "message": "Numero original nao informado", "status_code": 400}
        if not new_number:
            return {"ok": False, "message": "Numero nao informado", "status_code": 400}
        if not due_date:
            return {"ok": False, "message": "Vencimento nao informado", "status_code": 400}

        try:
            datetime.strptime(due_date.strip(), "%d/%m/%Y")
        except ValueError:
            return {"ok": False, "message": "Vencimento invalido. Use dd/mm/aaaa", "status_code": 400}

        resolved_original = self._resolve_original_number(original_number.strip())
        target_number = new_number.strip()
        client_name = (name or "Cliente").strip()
        client_login = str(login or "").strip()

        if target_number != original_number.strip():
            self.repo.upsert_number_override(resolved_original, target_number)
            self.repo.delete_panel_client(original_number.strip())

        self.repo.upsert_panel_client(target_number, client_name, due_date.strip(), client_login)
        # Manual edits should take precedence over stale override values from past payments.
        self.repo.upsert_due_date_override(resolved_original, due_date.strip())
        self.repo.unhide_client(target_number)
        return {"ok": True, "status_code": 200}

    def remove_client(self, number: str) -> Dict[str, Any]:
        if not number:
            return {"ok": False, "message": "Numero nao informado", "status_code": 400}

        self.repo.delete_panel_client(number.strip())
        self.repo.hide_client(number.strip())
        return {"ok": True, "status_code": 200}

    def sync_approved_payments(self, limit: int = 100) -> Dict[str, Any]:
        if not self.mp_client.enabled:
            return {"ok": False, "message": "MP_ACCESS_TOKEN nao configurado", "status_code": 400}

        payments, error = self.mp_client.search_payments(status="approved", limit=limit)
        if error:
            return {"ok": False, "message": error, "status_code": 500}

        imported = 0
        skipped = 0

        for payment in payments:
            payment_id = str(payment.get("id", "")).strip()
            if not payment_id:
                skipped += 1
                continue

            existing_payment = self.repo.get_payment(payment_id)
            external_reference = str(payment.get("external_reference", "")).strip()
            number = self.extract_client_number_from_payment(payment)

            if not number:
                skipped += 1
                continue

            amount = float(payment.get("transaction_amount", 0) or 0)
            status = str(payment.get("status", "pending")).strip()
            self.repo.save_payment(payment_id, external_reference, number, status, amount)
            if external_reference:
                self.repo.update_charge_status(external_reference, status, payment_id)

            if existing_payment is None:
                imported += 1
                if status == "approved":
                    self.update_due_date_after_approved_payment(number)

        return {
            "ok": True,
            "status_code": 200,
            "importados": imported,
            "ignorados": skipped,
            "total_consultado": len(payments),
        }

    def import_sheet_clients(self) -> Dict[str, Any]:
        return {
            "ok": False,
            "message": "Importacao por planilha desativada. Use apenas clientes salvos no banco/painel.",
            "status_code": 400,
        }

    def get_dispatch_settings(self) -> Dict[str, Any]:
        get_settings = getattr(self.repo, "get_dispatch_settings", None)
        if callable(get_settings):
            settings = get_settings()
        else:
            settings = {
                "habilitado": False,
                "horario_1": "08:00",
                "horario_2": "12:00",
                "horario_3": "18:00",
            }
        settings["ok"] = True
        settings["status_code"] = 200
        return settings

    def save_dispatch_settings(self, enabled: bool, time_1: str, time_2: str, time_3: str) -> Dict[str, Any]:
        times = [time_1, time_2, time_3]
        for value in times:
            try:
                datetime.strptime(str(value).strip(), "%H:%M")
            except ValueError:
                return {"ok": False, "message": "Horario invalido. Use HH:MM", "status_code": 400}

        save_settings = getattr(self.repo, "save_dispatch_settings", None)
        if callable(save_settings):
            save_settings(enabled, time_1.strip(), time_2.strip(), time_3.strip())
        return {"ok": True, "status_code": 200}

    def get_recent_dispatch_executions(self) -> Dict[str, Any]:
        get_executions = getattr(self.repo, "get_recent_dispatch_executions", None)
        return {
            "ok": True,
            "status_code": 200,
            "execucoes": get_executions() if callable(get_executions) else [],
        }

    def run_scheduled_dispatch(self, slot: str) -> Dict[str, Any]:
        today = self._today_br()
        was_executed = getattr(self.repo, "was_dispatch_executed", None)
        if callable(was_executed) and was_executed(today, slot):
            return {"ok": True, "status_code": 200, "enviados": 0, "ja_executado": True}

        result = self.send_manual_charges()
        if not result["ok"]:
            return result

        record_execution = getattr(self.repo, "record_dispatch_execution", None)
        if callable(record_execution):
            record_execution(today, slot, result["enviados"])
        result["ja_executado"] = False
        return result
