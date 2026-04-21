const API_URL = "http://127.0.0.1:8000";
let myChart = null;

async function fetchMarketStatus() {
    try {
        const response = await fetch(`${API_URL}/market-status`);
        const data = await response.json();
        console.log("Dados Recebidos:", data);
        renderMarketStatus(data);
        document.getElementById('connection-status').innerText = "API Online";
        document.getElementById('connection-status').style.color = "#0ecb81";
    } catch (error) {
        console.error("Erro na API:", error);
        document.getElementById('connection-status').innerText = "Erro de Conexão";
        document.getElementById('connection-status').style.color = "red";
    }
}

function renderMarketStatus(data) {
    const list = document.getElementById('status-list');
    list.innerHTML = "";

    data.forEach(item => {
        const card = document.createElement('div');
        card.className = 'ticker-card';
        
        // Lógica robusta para pegar nomes de colunas independente de Case
        const ticker = item.ticker || item.TICKER || "S/N";
        const preco = item.preco_fechamento || item.PRECO_FECHAMENTO || 0;
        const sinal = item.sinal_estrategia || item.SINAL_ESTRATEGIA || "NEUTRO";

        const signalClass = `signal-${sinal.toLowerCase().replace(" ", "-")}`;

        card.innerHTML = `
            <div style="display: flex; justify-content: space-between;">
                <strong>${ticker}</strong>
                <span>R$ ${Number(preco).toFixed(2)}</span>
            </div>
            <div class="${signalClass}" style="font-size: 0.85rem; margin-top: 5px;">
                ${sinal}
            </div>
        `;
        
        card.onclick = () => fetchTickerHistory(ticker);
        list.appendChild(card);
    });
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

function showAlert(message, isError = false) {
    const alertDiv = document.getElementById('custom-alert');
    const msgSpan = document.getElementById('alert-message');
    
    alertDiv.style.borderLeftColor = isError ? "#f6465d" : "#0ecb81";
    msgSpan.innerText = message;
    alertDiv.classList.remove('hidden');

    // Fecha sozinho após 5 segundos
    setTimeout(closeAlert, 5000);
}

function closeAlert() {
    document.getElementById('custom-alert').classList.add('hidden');
}

async function fetchTickerHistory(ticker) {
    console.log("Buscando dados de:", ticker); // Debug para ver se o clique pegou
    document.getElementById('current-ticker').innerText = `Carregando ${ticker}...`;
    
    try {
        const response = await fetch(`${API_URL}/history/${ticker}`);
        const data = await response.json();
        
        // O PULO DO GATO: Se o dado veio vazio, avise.
        if (!data || data.length === 0) {
            showAlert("Nenhum dado encontrado para este ativo.");
            return;
        }

        updateChart(ticker, data);
        document.getElementById('current-ticker').innerText = `Análise Técnica: ${ticker}`;
    } catch (error) {
        console.error("Erro ao buscar histórico:", error);
    }
}

function updateChart(ticker, history) {
    const ctx = document.getElementById('mainChart').getContext('2d');
    
    // Pegamos os últimos 60 dias para um zoom de qualidade
    const recent = history.slice(-60);

    const labels = recent.map(h => h.data_referencia || h.DATA_REFERENCIA);
    const prices = recent.map(h => h.preco_fechamento || h.PRECO_FECHAMENTO);
    const b_sup = recent.map(h => h.bband_superior || h.BBAND_SUPERIOR);
    const b_inf = recent.map(h => h.bband_inferior || h.BBAND_INFERIOR);

    // Cálculo dinâmico de limites para evitar que o gráfico "fique no rodapé"
    const minPrice = Math.min(...prices.filter(p => p > 0)) * 0.98;
    const maxPrice = Math.max(...prices) * 1.02;

    if (myChart) { myChart.destroy(); }

    myChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Preço',
                    data: prices,
                    borderColor: '#f0b90b',
                    borderWidth: 3,
                    pointRadius: 0,
                    tension: 0.1,
                    zIndex: 2
                },
                {
                    label: 'Bandas',
                    data: b_sup,
                    borderColor: 'rgba(255, 255, 255, 0.1)',
                    borderWidth: 1,
                    pointRadius: 0,
                    fill: false
                },
                {
                    label: 'Banda Inf',
                    data: b_inf,
                    borderColor: 'rgba(255, 255, 255, 0.1)',
                    borderWidth: 1,
                    pointRadius: 0,
                    backgroundColor: 'rgba(240, 185, 11, 0.05)',
                    fill: '-1' 
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    position: 'right',
                    min: minPrice, // Força o início do eixo perto do preço mínimo
                    max: maxPrice, // Força o fim perto do máximo
                    ticks: { color: '#848e9c' },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                x: {
                    ticks: { color: '#848e9c', maxTicksLimit: 10 },
                    grid: { display: false }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });
}

fetchMarketStatus();