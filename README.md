# Bot de Cobrança Automática com WhatsApp e Mercado Pago

Sistema profissional para gerenciar cobranças automáticas de TV por assinatura via WhatsApp, integrado com Mercado Pago e Google Sheets.

## Funcionalidades

- ✅ Envio automático de cobranças via WhatsApp
- ✅ Link de pagamento Mercado Pago (Checkout)
- ✅ Validação de proposta com código único (external_reference)
- ✅ Dashboard protegido por login/senha
- ✅ Visualização de clientes (status pago/não pago)
- ✅ Histórico de cobrancas com rastreio
- ✅ Atualização automática de vencimento após pagamento
- ✅ Saldo recebido e pendente em cards
- ✅ Sincronia com Google Sheets

## Pré-requisitos

- Python 3.8+
- Conta Z-API (WhatsApp)
- Conta Mercado Pago (Seller)
- Google Cloud (para integração Google Sheets - opcional)

## Instalação

1. Clone ou baixe o projeto:
```bash
cd seu-projeto
```

2. Instale dependências:
```bash
pip install -r requirements.txt
```

3. Configure variáveis de ambiente (crie um arquivo `.env` baseado em `.env.example`):
```bash
cp .env.example .env
```

4. Edite `.env` com suas credenciais reais.

## Como usar

### 1. Rodar localmente
```bash
python app.py
```
Acesse: `http://localhost:10000/login`

### 2. Estrutura das rotas

**Públicas:**
- `GET /` - Informações da aplicação
- `GET /health` - Status da aplicação
- `POST /webhook` - Webhook da Z-API (WhatsApp)
- `POST /webhook/mercadopago` - Webhook do Mercado Pago

**Autenticadas (login obrigatório):**
- `GET /login` - Tela de login
- `GET /logout` - Desconectar
- `GET /dashboard` - Painel principal
- `GET /api/clientes-status` - API de status dos clientes
- `GET /api/cobrancas` - API de lista de cobranças com códigos

**Manuais:**
- `POST /mercadopago/criar-pagamento` - Criar cobrança manualmente
- `GET /enviar-cobrancas` - Disparar cobranças do dia

### 3. Estrutura do banco de dados

Tabelas automáticas (SQLite):
- `interacoes` - Registro de interações diárias
- `pagamentos` - Histórico de pagamentos aprovados/pendentes
- `vencimentos_override` - Vencimentos atualizados após pagamento
- `cobrancas` - Cada cobrança gerada com seu código único

### 4. Fluxo de pagamento

1. Cobrança é criada com código único (8 primeiros caracteres do UUID)
2. Cliente recebe mensagem no WhatsApp com:
   - Link de pagamento do Mercado Pago
   - Código da cobrança (para referência)
3. Cliente paga pelo link
4. Webhook do Mercado Pago chega confirmando pagamento
5. Sistema:
   - Salva o pagamento no banco
   - Identifica cliente pelo código da cobrança (não depende do nome do pagador)
   - Envia confirmação no WhatsApp
   - Atualiza vencimento para +1 mês (se Google Sheets estiver configurado)

## Configuração detalhada

### Z-API
1. Acesse `https://z-api.io`
2. Crie uma instância
3. Copie `INSTANCE_ID` e `TOKEN`
4. Configure webhook apontando para `/webhook`

### Mercado Pago
1. Acesse `https://mercadopago.com.br`
2. Vá em Configurações > Credenciais
3. Copie seu `Access Token`
4. Em Integrações > Webhooks, configure:
   - URL: `https://seu-dominio.com/webhook/mercadopago`
   - Evento: `payment`

### Google Sheets (Atualizar vencimento automaticamente)
1. Acesse `https://console.cloud.google.com`
2. Crie um projeto
3. Ative `Google Sheets API`
4. Crie `Service Account`
5. Gere arquivo JSON de credenciais
6. Coloque o JSON em um local seguro
7. Compartilhe a planilha com o e-mail da Service Account (permissão: editor)
8. Configure as variáveis:
   - `GOOGLE_SPREADSHEET_ID` (ID da URL da planilha)
   - `GOOGLE_WORKSHEET_NAME` (nome da aba, ex: Pagina1)
   - `GOOGLE_SERVICE_ACCOUNT_FILE` (caminho do JSON)

### Autenticação do Dashboard
- `ADMIN_USERNAME` - Usuário (padrão: admin)
- `ADMIN_PASSWORD` - Senha (padrão: 123456)
- `FLASK_SECRET_KEY` - Chave secreta para sessões (mude em produção!)

## Deploy

### Render.com (recomendado)
1. Push código para GitHub
2. Conecte repositório em Render
3. Configure variáveis de ambiente
4. Deploy automático

### Heroku
```bash
heroku create seu-app
heroku config:set ADMIN_USERNAME=seu_usuario
heroku config:set ADMIN_PASSWORD=sua_senha
git push heroku main
```

### Windows (local/servidor)
Use `python-dotenv` para carregar `.env` automaticamente.

## Estrutura de arquivos

```
.
├── app.py                      # Aplicação Flask principal
├── config.py                   # Configurações (variáveis de ambiente)
├── database.py                 # Repositório SQLite
├── requirements.txt            # Dependências Python
├── services/
│   ├── __init__.py
│   ├── billing_service.py      # Lógica de cobranças
│   ├── zapi_service.py         # Cliente Z-API
│   ├── mercadopago_service.py  # Cliente Mercado Pago
│   └── google_sheets_service.py# Cliente Google Sheets
├── templates/
│   ├── login.html              # Tela de login
│   └── dashboard.html          # Painel principal
├── static/
│   ├── login.css
│   ├── dashboard.css
│   └── dashboard.js
├── controle.db                 # Banco SQLite (criado automaticamente)
└── .env.example                # Exemplo de variáveis
```

## Dashboard

### Aba Clientes
- Total de clientes
- Clientes pagos / Não pagos
- Saldo recebido / Pendente / Líquido
- Tabela com status de cada cliente

### Aba Cobrancas
- Filtro: Todas / Pendentes / Aprovadas
- Código da cobrança (primeiros 8 caracteres)
- Cliente e número
- Valor
- Status de reconciliação
- Data de criação

## Troubleshooting

**Erro: "external_reference não encontrado"**
- Certifique que MP_ACCESS_TOKEN está correto
- Webhook do Mercado Pago está funcionando

**Cobrança não atualiza vencimento**
- Sem Google Sheets configurado? Sistema salva localmente no `vencimentos_override`
- Com Google Sheets? Verifique credenciais e permissões de acesso

**Dashboard não carrega**
- Você está logado? Verifique cookies/sessão
- Check console (F12) para erros de rede

**WhatsApp não recebe mensagens**
- Z-API está ativa?
- Token e Instance ID estão corretos?
- Webhook da Z-API está apontando para sua URL?

## Roadmap futuro

- [ ] Autenticação via código QR (Z-API)
- [ ] Suporte a múltiplos administradores
- [ ] Relatórios avançados (gráficos de pagamento por período)
- [ ] Integração com outros gateways (PagSeguro, Stripe)
- [ ] Sistema de multa/juros para atraso
- [ ] Envio via email também

## Suporte

Para dúvidas ou bugs, verifique os logs da aplicação ou entre em contato.

---

**Versão:** v7 (com rastreio por código de cobrança)  
**Última atualização:** Abril 2026
