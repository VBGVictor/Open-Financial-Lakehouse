// 1. Definição da URL da API (Faltava isso no seu!)
const API_URL = "http://127.0.0.1:8000";

// 2. Funções de Alerta (Devem vir primeiro para estarem prontas)
function closeAlert() {
    const alertDiv = document.getElementById('custom-alert');
    if (alertDiv) alertDiv.classList.add('hidden');
}

function showAlert(message, isError = false) {
    const alertDiv = document.getElementById('custom-alert');
    const msgSpan = document.getElementById('alert-message');
    
    if (!alertDiv || !msgSpan) return; // Segurança caso o HTML não tenha os IDs

    alertDiv.style.borderLeft = isError ? "5px solid #f6465d" : "5px solid #0ecb81";
    msgSpan.innerText = message;
    alertDiv.classList.remove('hidden');

    // Fecha sozinho após 6 segundos
    setTimeout(closeAlert, 6000);
}

async function requestIngestion() {
    const tickerInput = document.getElementById('new-ticker');
    const ticker = tickerInput.value.trim().toUpperCase();
    
    if(!ticker) return showAlert("Digite um ticker!", true);
    
    showAlert(`Iniciando processamento de ${ticker}...`);

    try {
        // Garanta que API_URL está definido como "http://127.0.0.1:8000"
        const response = await fetch(`${API_URL}/ingest/${ticker}`, { method: 'POST' });
        const result = await response.json();

        if (result.status === "success") {
            showAlert(result.message);
            // Se estiver no index (terminal), atualiza a lista
            if (typeof fetchMarketStatus === "function") {
                await fetchMarketStatus();
            }
        } else {
            showAlert(result.message, true);
        }
    } catch (error) {
        showAlert("Erro de conexão com o servidor.", true);
    }
}

// 3. Variável Global para o Gráfico (Para podermos destruir e criar um novo)
let chartInstance = null;

// 4. Buscar Status do Mercado (Preenche o painel lateral)
async function fetchMarketStatus() {
    try {
        const response = await fetch(`${API_URL}/market-status`);
        const data = await response.json();
        
        const statusList = document.getElementById('status-list');
        statusList.innerHTML = ''; // Limpa a lista antes de preencher

        data.forEach(item => {
            const card = document.createElement('div');
            card.className = 'status-card';
            card.onclick = () => loadTickerHistory(item.ticker);
            
            const colorClass = item.variacao_diaria_pct >= 0 ? 'text-up' : 'text-down';
            
            card.innerHTML = `
                <div class="card-info">
                    <span class="ticker-name">${item.ticker}</span>
                    <span class="price">R$ ${item.preco_fechamento.toFixed(2)}</span>
                </div>
                <div class="card-metric ${colorClass}">
                    ${item.variacao_diaria_pct.toFixed(2)}%
                </div>
            `;
            statusList.appendChild(card);
        });
    } catch (error) {
        console.error("Erro ao carregar Market Status:", error);
    }
}

// 5. Carregar Histórico e Gerar Gráfico
async function loadTickerHistory(ticker) {
    document.getElementById('current-ticker').innerText = ticker;
    
    try {
        const response = await fetch(`${API_URL}/history/${ticker}`);
        const data = await response.json();

        const labels = data.map(d => new Date(d.data_referencia).toLocaleDateString());
        const prices = data.map(d => d.preco_fechamento);

        renderChart(labels, prices, ticker);
    } catch (error) {
        showAlert("Erro ao carregar histórico do ativo.", true);
    }
}

// 6. Função de Renderização do Chart.js
function renderChart(labels, prices, ticker) {
    const ctx = document.getElementById('mainChart').getContext('2d');
    
    if (chartInstance) {
        chartInstance.destroy(); // Remove o gráfico anterior se existir
    }

    chartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: `Preço de Fechamento - ${ticker}`,
                data: prices,
                borderColor: '#0ecb81',
                backgroundColor: 'rgba(14, 203, 129, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: { beginAtZero: false, grid: { color: '#333' } },
                x: { grid: { color: '#333' } }
            },
            plugins: {
                legend: { labels: { color: '#fff' } }
            }
        }
    });
}

// 7. Inicialização ao carregar a página
window.onload = () => {
    fetchMarketStatus();
};