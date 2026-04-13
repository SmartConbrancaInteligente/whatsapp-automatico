

// ...existing code...


// Função para buscar clientes do backend e preencher a tabela
async function carregarClientes() {
    try {
        const resp = await fetch('/api/clientes-status');
        if (!resp.ok) throw new Error('Erro ao buscar clientes');
        const data = await resp.json();
        const clientes = data.clientes || [];
        const tbody = document.getElementById('clientesTbody');
        tbody.innerHTML = '';
        if (clientes.length === 0) {
            document.getElementById('emptyState').classList.remove('hidden');
            return;
        } else {
            document.getElementById('emptyState').classList.add('hidden');
        }
        clientes.forEach(cliente => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${cliente.nome || ''}</td>
                <td>${cliente.login || ''}</td>
                <td>${cliente.numero || ''}</td>
                <td>${cliente.vencimento || ''}</td>
                <td>${cliente.status || ''}</td>
                <td><!-- ações futuras --></td>
            `;
            tbody.appendChild(tr);
        });
    } catch (e) {
        document.getElementById('emptyState').classList.remove('hidden');
    }
}

// Alternância de abas do painel (deve estar no final do arquivo)
document.addEventListener('DOMContentLoaded', function() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabButtons.forEach(btn => {
        btn.addEventListener('click', function() {
            tabButtons.forEach(b => b.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));
            this.classList.add('active');
            const tabId = this.getAttribute('data-tab');
            const content = document.getElementById(tabId + '-tab');
            if (content) {
                content.classList.add('active');
                if (tabId === 'clientes') carregarClientes();
            }
        });
    });
    // Carregar clientes ao abrir a página na aba Clientes
    if (document.querySelector('.tab-btn.active[data-tab="clientes"]')) {
        carregarClientes();
    }
});