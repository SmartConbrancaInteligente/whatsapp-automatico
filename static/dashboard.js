const state = {
    clientes: [],
    overdueClients: [],
    editingNumber: null,
    clientSearch: "",
    clientDateFrom: "",
    clientDateTo: "",
};

async function apiJson(url, options = {}) {
    const fetchOptions = {
        cache: "no-store",
        ...options,
    };
    const response = await fetch(url, fetchOptions);
    const data = await response.json().catch(() => ({}));

    if (response.status === 401) {
        window.location.href = "/login";
        throw new Error("nao autenticado");
    }

    if (!response.ok) {
        throw new Error(data.erro || "Erro na requisicao");
    }

    return data;
}

function showMessage(message, type = "info") {
    const el = document.getElementById("statusMessage");
    el.textContent = message;
    el.className = `status-message ${type}`;
    el.classList.remove("hidden");
}

function clearMessage() {
    const el = document.getElementById("statusMessage");
    el.textContent = "";
    el.className = "status-message hidden";
}

function escapeHtml(value) {
    return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function formatCurrency(value) {
    return new Intl.NumberFormat("pt-BR", {
        style: "currency",
        currency: "BRL",
    }).format(Number(value) || 0);
}

function parseBrDate(value) {
    const [day, month, year] = String(value || "").split("/").map(Number);
    if (!day || !month || !year) {
        return null;
    }

    const parsed = new Date(year, month - 1, day);
    if (Number.isNaN(parsed.getTime())) {
        return null;
    }

    parsed.setHours(0, 0, 0, 0);
    return parsed;
}

function parseIsoDate(value) {
    if (!value) {
        return null;
    }

    const [year, month, day] = String(value).split("-").map(Number);
    if (!day || !month || !year) {
        return null;
    }

    const parsed = new Date(year, month - 1, day);
    if (Number.isNaN(parsed.getTime())) {
        return null;
    }

    parsed.setHours(0, 0, 0, 0);
    return parsed;
}

function resetClientForm() {
    state.editingNumber = null;
    document.getElementById("clienteForm").reset();
    document.getElementById("clienteLogin").value = "";
    document.getElementById("clienteNumero").readOnly = false;
    document.getElementById("clientFormTitle").textContent = "Adicionar cliente";
    document.getElementById("saveClientBtn").textContent = "Salvar cliente";
    document.getElementById("cancelEditBtn").classList.add("hidden");
}

function startEditClient(number) {
    const cliente = state.clientes.find((item) => item.numero === number);
    if (!cliente) {
        return;
    }

    state.editingNumber = cliente.numero_original || number;
    document.getElementById("clientFormTitle").textContent = `Editar ${cliente.nome || "cliente"}`;
    document.getElementById("saveClientBtn").textContent = "Atualizar cliente";
    document.getElementById("cancelEditBtn").classList.remove("hidden");
    document.getElementById("clienteNome").value = cliente.nome || "";
    document.getElementById("clienteLogin").value = cliente.login || "";
    document.getElementById("clienteNumero").value = cliente.numero || "";
    document.getElementById("clienteNumero").readOnly = false;
    document.getElementById("clienteVencimento").value = cliente.vencimento || "";
    clearMessage();
}

async function carregarClientes() {
    const emptyState = document.getElementById("emptyState");

    try {
        const data = await apiJson("/api/clientes-status");
        const clientes = data.clientes || [];
        const resumo = data.resumo || {};
        state.clientes = clientes;

        document.getElementById("totalClientes").textContent = clientes.length;

        let pagos = 0;
        let naoPagos = 0;

        clientes.forEach((cliente) => {
            if (cliente.status_pagamento === "pago") {
                pagos += 1;
            } else {
                naoPagos += 1;
            }
        });

        renderClientes();

        document.getElementById("totalPagos").textContent = pagos;
        document.getElementById("totalNaoPagos").textContent = naoPagos;
        document.getElementById("saldoRecebido").textContent = formatCurrency(resumo.total_recebido || 0);
        document.getElementById("saldoPendente").textContent = formatCurrency(resumo.total_pendente || 0);
        document.getElementById("saldoLiquido").textContent = formatCurrency(resumo.saldo_liquido || 0);
    } catch (err) {
        emptyState.textContent = err.message || "Erro ao carregar clientes.";
        emptyState.classList.remove("hidden");
    }
}

function renderClientes() {
    const tbody = document.getElementById("clientesTbody");
    const emptyState = document.getElementById("emptyState");
    const search = state.clientSearch.toLowerCase();
    const fromDate = parseIsoDate(state.clientDateFrom);
    const toDate = parseIsoDate(state.clientDateTo);
    tbody.innerHTML = "";

    const clientesFiltrados = state.clientes.filter((cliente) => {
        const dueDate = parseBrDate(cliente.vencimento);

        if (search) {
            const nome = String(cliente.nome || "").toLowerCase();
            const numero = String(cliente.numero || "").toLowerCase();
            if (!nome.includes(search) && !numero.includes(search)) {
                return false;
            }
        }

        if (fromDate && (!dueDate || dueDate < fromDate)) {
            return false;
        }

        if (toDate && (!dueDate || dueDate > toDate)) {
            return false;
        }

        return true;
    }).sort((left, right) => {
        const leftDate = parseBrDate(left.vencimento);
        const rightDate = parseBrDate(right.vencimento);

        if (leftDate && rightDate && leftDate.getTime() !== rightDate.getTime()) {
            return leftDate.getTime() - rightDate.getTime();
        }
        if (leftDate) {
            return -1;
        }
        if (rightDate) {
            return 1;
        }
        return String(left.nome || "").localeCompare(String(right.nome || ""), "pt-BR");
    });

    clientesFiltrados.forEach((cliente) => {
        const status = cliente.status_pagamento === "pago" ? "pago" : "nao_pago";
        const origem = "painel";
        const proximoStatus = status === "pago" ? "pending" : "approved";
        const textoBotaoStatus = status === "pago" ? "Marcar nao pago" : "Marcar pago";
        const originalNumber = cliente.numero_original || cliente.numero || "";

        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>
                <strong>${escapeHtml(cliente.nome || "Cliente")}</strong>
                <div class="meta-chip-group">
                    <span class="origin-badge ${origem}">Painel</span>
                </div>
            </td>
            <td>${escapeHtml(cliente.login || "-")}</td>
            <td>${escapeHtml(cliente.numero || "-")}</td>
            <td>${escapeHtml(cliente.vencimento || "-")}</td>
            <td><span class="badge ${status}">${status === "pago" ? "Pago" : "Nao pago"}</span></td>
            <td class="actions-cell">
                <button class="table-btn" data-action="edit" data-number="${escapeHtml(cliente.numero || "")}">Editar</button>
                <button class="table-btn payment-toggle ${status}" data-action="toggle-payment" data-number="${escapeHtml(cliente.numero || "")}" data-original="${escapeHtml(originalNumber)}" data-status="${escapeHtml(proximoStatus)}">${escapeHtml(textoBotaoStatus)}</button>
                <button class="table-btn danger" data-action="delete" data-number="${escapeHtml(cliente.numero || "")}">Remover</button>
            </td>
        `;
        tbody.appendChild(tr);
    });

    if (clientesFiltrados.length > 0) {
        emptyState.classList.add("hidden");
        return;
    }

    emptyState.textContent = (search || fromDate || toDate)
        ? "Nenhum cliente encontrado para os filtros informados."
        : "Nenhum cliente encontrado.";
    emptyState.classList.remove("hidden");
}

async function carregarCobrancas() {
    const tbody = document.getElementById("cobrancasTbody");
    const emptyMsg = document.getElementById("emptyCobrancas");
    tbody.innerHTML = "";

    try {
        const data = await apiJson("/api/cobrancas");
        const cobrancas = data.cobrancas || [];
        const filtroAtivo = document.querySelector(".filter-btn.active").dataset.filter;

        let filtradas = cobrancas;
        if (filtroAtivo !== "all") {
            filtradas = cobrancas.filter((c) => c.status === filtroAtivo);
        }

        filtradas.forEach((cobranca) => {
            const statusClass = cobranca.status === "approved" ? "pago" : "nao_pago";
            const statusText = cobranca.status === "approved" ? "Aprovado" : "Pendente";
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td><strong>${escapeHtml(cobranca.codigo)}</strong></td>
                <td>${escapeHtml(cobranca.nome || "Cliente")}</td>
                <td>${escapeHtml(cobranca.numero || "-")}</td>
                <td>${formatCurrency(cobranca.valor)}</td>
                <td><span class="badge ${statusClass}">${statusText}</span></td>
                <td>${escapeHtml(cobranca.criado_em || "-")}</td>
            `;
            tbody.appendChild(tr);
        });

        emptyMsg.classList.toggle("hidden", filtradas.length > 0);
    } catch (err) {
        emptyMsg.textContent = err.message || "Erro ao carregar cobrancas.";
        emptyMsg.classList.remove("hidden");
    }
}

async function carregarDisparos() {
    await Promise.all([
        carregarConfigDisparos(),
        carregarClientesAtrasados(),
        carregarExecucoesDisparo(),
    ]);
}

async function carregarConfigDisparos() {
    const data = await apiJson("/api/config-disparos");
    document.getElementById("dispatchEnabled").checked = Boolean(data.habilitado);
    document.getElementById("dispatchTime1").value = data.horario_1 || "08:00";
    document.getElementById("dispatchTime2").value = data.horario_2 || "12:00";
    document.getElementById("dispatchTime3").value = data.horario_3 || "18:00";
}

async function carregarClientesAtrasados() {
    const data = await apiJson("/api/clientes-atrasados");
    state.overdueClients = data.clientes || [];
    renderOverdueClients();
}

function renderOverdueClients() {
    const tbody = document.getElementById("overdueClientsTbody");
    const empty = document.getElementById("emptyOverdueClients");
    const selectAll = document.getElementById("selectAllOverdue");
    tbody.innerHTML = "";
    selectAll.checked = false;

    state.overdueClients.forEach((cliente) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td><input class="overdue-checkbox" type="checkbox" value="${escapeHtml(cliente.numero)}" /></td>
            <td>${escapeHtml(cliente.nome || "Cliente")}</td>
            <td>${escapeHtml(cliente.numero || "-")}</td>
            <td>${escapeHtml(cliente.vencimento || "-")}</td>
            <td>${escapeHtml(cliente.dias_atraso)}</td>
            <td><span class="overdue-origin">${escapeHtml(cliente.origem || "painel")}</span></td>
        `;
        tbody.appendChild(tr);
    });

    empty.classList.toggle("hidden", state.overdueClients.length > 0);
}

async function carregarExecucoesDisparo() {
    const data = await apiJson("/api/disparos/execucoes");
    const tbody = document.getElementById("dispatchRunsTbody");
    const empty = document.getElementById("emptyDispatchRuns");
    tbody.innerHTML = "";

    const runs = data.execucoes || [];
    runs.forEach((run) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${escapeHtml(run.data || "-")}</td>
            <td>${escapeHtml(run.slot || "-")}</td>
            <td>${escapeHtml(run.executado_em || "-")}</td>
            <td>${escapeHtml(run.total_enviados || 0)}</td>
        `;
        tbody.appendChild(tr);
    });

    empty.classList.toggle("hidden", runs.length > 0);
}

async function salvarCliente(event) {
    event.preventDefault();
    clearMessage();

    const payload = {
        nome: document.getElementById("clienteNome").value.trim(),
        login: document.getElementById("clienteLogin").value.trim(),
        numero: document.getElementById("clienteNumero").value.trim(),
        vencimento: document.getElementById("clienteVencimento").value.trim(),
    };

    try {
        if (state.editingNumber) {
            await apiJson(`/api/clientes/${encodeURIComponent(state.editingNumber)}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            showMessage("Cliente atualizado no painel.", "success");
        } else {
            await apiJson("/api/clientes", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            showMessage("Cliente salvo no painel.", "success");
        }

        resetClientForm();
        await carregarClientes();
        await carregarClientesAtrasados();
    } catch (err) {
        showMessage(err.message || "Nao foi possivel salvar o cliente.", "error");
    }
}

async function removerCliente(number) {
    if (!number) {
        return;
    }

    const confirmed = window.confirm("Remover este cliente do painel?");
    if (!confirmed) {
        return;
    }

    clearMessage();
    try {
        await apiJson(`/api/clientes/${encodeURIComponent(number)}`, { method: "DELETE" });
        if (state.editingNumber === number) {
            resetClientForm();
        }
        showMessage("Cliente removido do dashboard.", "success");
        await carregarClientes();
        await carregarClientesAtrasados();
    } catch (err) {
        showMessage(err.message || "Nao foi possivel remover o cliente.", "error");
    }
}

async function sincronizarPagamentos() {
    const button = document.getElementById("syncPaymentsBtn");
    const limit = Number(document.getElementById("syncLimit").value || 100);
    button.disabled = true;
    clearMessage();

    try {
        const data = await apiJson("/api/sincronizar-pagamentos", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ limit }),
        });
        showMessage(
            `Sincronizacao concluida: ${data.importados || 0} importados, ${data.ignorados || 0} ignorados, ${data.total_consultado || 0} consultados.`,
            "success"
        );
        await carregarClientes();
        await carregarCobrancas();
        await carregarClientesAtrasados();
    } catch (err) {
        showMessage(err.message || "Nao foi possivel sincronizar pagamentos.", "error");
    } finally {
        button.disabled = false;
    }
}

async function salvarConfigDisparos(event) {
    event.preventDefault();
    clearMessage();

    try {
        await apiJson("/api/config-disparos", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                habilitado: document.getElementById("dispatchEnabled").checked,
                horario_1: document.getElementById("dispatchTime1").value,
                horario_2: document.getElementById("dispatchTime2").value,
                horario_3: document.getElementById("dispatchTime3").value,
            }),
        });
        showMessage("Configuracao de disparos salva.", "success");
        await carregarExecucoesDisparo();
    } catch (err) {
        showMessage(err.message || "Nao foi possivel salvar os horarios.", "error");
    }
}

function getSelectedOverdueNumbers() {
    return Array.from(document.querySelectorAll(".overdue-checkbox:checked")).map((item) => item.value);
}

async function enviarDisparosManuais(numbers) {
    clearMessage();
    try {
        const earlyPayment = document.getElementById("earlyPaymentCheckbox")?.checked;
        const data = await apiJson("/api/disparos/manual", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ numeros: numbers, early_payment: !!earlyPayment }),
        });
        showMessage(
            `Disparo concluido: ${data.enviados || 0} enviados, ${data.ignorados_interacao || 0} ignorados por interacao no dia.`,
            "success"
        );
        await carregarCobrancas();
        await carregarClientesAtrasados();
        await carregarExecucoesDisparo();
    } catch (err) {
        showMessage(err.message || "Nao foi possivel enviar os disparos.", "error");
    }
}

function initTabs() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", async () => {
            const tabName = btn.dataset.tab;

            document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
            document.querySelectorAll(".tab-content").forEach((t) => t.classList.remove("active"));

            btn.classList.add("active");
            document.getElementById(tabName + "-tab").classList.add("active");

            if (tabName === "cobrancas") {
                await carregarCobrancas();
            }
            if (tabName === "disparos") {
                await carregarDisparos();
            }
        });
    });
}

function initChargeFilters() {
    document.querySelectorAll(".filter-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".filter-btn").forEach((b) => b.classList.remove("active"));
            btn.classList.add("active");
            carregarCobrancas();
        });
    });
}

function initClientFilters() {
    document.getElementById("clientSearchInput").addEventListener("input", (event) => {
        state.clientSearch = event.target.value.trim();
        renderClientes();
    });
    document.getElementById("clientDateFrom").addEventListener("input", (event) => {
        state.clientDateFrom = event.target.value;
        renderClientes();
    });
    document.getElementById("clientDateTo").addEventListener("input", (event) => {
        state.clientDateTo = event.target.value;
        renderClientes();
    });
    document.getElementById("clearClientFiltersBtn").addEventListener("click", () => {
        state.clientSearch = "";
        state.clientDateFrom = "";
        state.clientDateTo = "";
        document.getElementById("clientSearchInput").value = "";
        document.getElementById("clientDateFrom").value = "";
        document.getElementById("clientDateTo").value = "";
        renderClientes();
    });
}

function initClientActions() {
    document.getElementById("clienteForm").addEventListener("submit", salvarCliente);
    document.getElementById("cancelEditBtn").addEventListener("click", resetClientForm);
    document.getElementById("syncPaymentsBtn").addEventListener("click", sincronizarPagamentos);
    document.getElementById("clientesTbody").addEventListener("click", (event) => {
        const button = event.target.closest("button[data-action]");
        if (!button) {
            return;
        }

        const { action, number, original, status } = button.dataset;
        if (action === "edit") {
            startEditClient(number);
            return;
        }

        if (action === "toggle-payment") {
            atualizarStatusPagamento(original || number, number, status);
            return;
        }

        if (action === "delete") {
            removerCliente(number);
        }
    });
}



async function atualizarStatusPagamento(numeroOriginal, numeroAtual, status) {
    const textoAcao = status === "approved" ? "marcar como pago" : "marcar como nao pago";
    const confirmado = window.confirm(`Confirma ${textoAcao}?`);
    if (!confirmado) {
        return;
    }

    clearMessage();
    try {
        await apiJson(`/api/clientes/${encodeURIComponent(numeroOriginal)}/status-pagamento`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ numero_atual: numeroAtual, status }),
        });
        showMessage("Status de pagamento atualizado manualmente.", "success");
        await carregarClientes();
        await carregarCobrancas();
        await carregarClientesAtrasados();
    } catch (err) {
        showMessage(err.message || "Nao foi possivel atualizar o status de pagamento.", "error");
    }
}

function initDispatchActions() {
    document.getElementById("dispatchConfigForm").addEventListener("submit", salvarConfigDisparos);
    document.getElementById("sendSelectedBtn").addEventListener("click", () => {
        const numbers = getSelectedOverdueNumbers();
        if (numbers.length === 0) {
            showMessage("Selecione pelo menos um cliente em atraso.", "error");
            return;
        }
        enviarDisparosManuais(numbers);
    });
    document.getElementById("sendAllOverdueBtn").addEventListener("click", () => {
        const numbers = state.overdueClients.map((item) => item.numero);
        if (numbers.length === 0) {
            showMessage("Nao ha clientes em atraso para enviar.", "error");
            return;
        }
        enviarDisparosManuais(numbers);
    });
    document.getElementById("selectAllOverdue").addEventListener("change", (event) => {
        document.querySelectorAll(".overdue-checkbox").forEach((item) => {
            item.checked = event.target.checked;
        });
    });
}

document.getElementById("refreshBtn").addEventListener("click", async () => {
    const activeTab = document.querySelector(".tab-btn.active").dataset.tab;
    if (activeTab === "clientes") {
        await carregarClientes();
        return;
    }
    if (activeTab === "cobrancas") {
        await carregarCobrancas();
        return;
    }
    await carregarDisparos();
});

initTabs();
initChargeFilters();
initClientFilters();
initClientActions();
initDispatchActions();
resetClientForm();
carregarClientes();
setInterval(() => {
    const activeTab = document.querySelector(".tab-btn.active")?.dataset.tab;
    if (activeTab === "clientes") {
        carregarClientes();
    }
}, 30000);

// Preencher o seletor de clientes no card de envio manual
function preencherSelectManualPaymentClientes() {
    const select = document.getElementById("manualPaymentClientSelect");
    if (!select) return;
    // Limpa opções antigas
    select.innerHTML = '<option value="">Selecione o cliente</option>';
    state.clientes.forEach((cliente) => {
        const nome = cliente.nome || "Cliente";
        const numero = cliente.numero || "";
        if (!numero) return;
        const option = document.createElement("option");
        option.value = numero;
        option.textContent = `${nome} (${numero})`;
        select.appendChild(option);
    });
}

// Ao carregar clientes, também preenche o select manual
const _carregarClientesOriginal = carregarClientes;
carregarClientes = async function() {
    await _carregarClientesOriginal.apply(this, arguments);
    preencherSelectManualPaymentClientes();
};

// Handler do envio manual do link de pagamento
const manualPaymentForm = document.getElementById("manualPaymentLinkForm");
if (manualPaymentForm) {
    manualPaymentForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        const select = document.getElementById("manualPaymentClientSelect");
        const numero = select.value;
        const nome = select.options[select.selectedIndex]?.text?.split(" (")[0] || "Cliente";
        if (!numero) {
            showMessage("Selecione um cliente.", "error");
            return;
        }
        const btn = document.getElementById("sendManualPaymentLinkBtn");
        btn.disabled = true;
        btn.textContent = "Enviando...";
        try {
            await apiJson("/api/enviar-link-pagamento", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ numero, nome })
            });
            showMessage("Link de pagamento enviado com sucesso!", "success");
        } catch (err) {
            showMessage(err.message || "Erro ao enviar link de pagamento.", "error");
        } finally {
            btn.disabled = false;
            btn.textContent = "Enviar link de pagamento";
        }
    });
}
