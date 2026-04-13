
// Alternância de abas do painel
document.addEventListener('DOMContentLoaded', function() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabButtons.forEach(btn => {
        btn.addEventListener('click', function() {
            // Remove classe 'active' de todos os botões e conteúdos
            tabButtons.forEach(b => b.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));

            // Adiciona classe 'active' ao botão clicado e ao conteúdo correspondente
            this.classList.add('active');
            const tabId = this.getAttribute('data-tab');
            const content = document.getElementById(tabId + '-tab');
            if (content) {
                content.classList.add('active');
            }
        });
    });
});