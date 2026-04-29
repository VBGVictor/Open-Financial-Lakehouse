const API_URL = "http://127.0.0.1:8000";
let myChart = null;
let marketInterval = null; // Trava para evitar múltiplos loops

async function fetchMarketStatus() {
    try {
        const response = await fetch(`${API_URL}/market-status?t=${Date.now()}`);
        const data = await response.json();
        
        // 1. FILTRO DE UNICIDADE (O Pulo do Gato para não duplicar na lateral)
        const filteredMap = {};
        data.forEach(item => {
            filteredMap[item.ticker] = item;
        });
        
        const uniqueAssets = Object.values(filteredMap);
        
        console.log("📊 Ativos Originais:", data.length);
        console.log("✅ Ativos Únicos:", uniqueAssets.length);

        // 2. Chamamos a renderização protegida
        renderMarketStatus(uniqueAssets);

        // 3. Atualiza status da API com estilo Tailwind
        const connStatus = document.getElementById('connection-status');
        if (connStatus) {
            connStatus.innerText = "API ONLINE";
            connStatus.className = "bg-green-500/10 text-green-500 text-[10px] font-bold px-3 py-1 rounded-full border border-green-500/20";
        }
    } catch (error) {
        console.error("Erro na API:", error);
    }
}

function renderMarketStatus(assets) {
    const statusList = document.getElementById('status-list');
    if (!statusList) return;

    // 1. O PULO DO GATO: Gerenciamento por ID Único
    assets.forEach(item => {
        // Criamos um ID baseado no ticker (ex: card-VALE3)
        const cardId = `card-${item.ticker}`;
        let card = document.getElementById(cardId);

        // Se o card NÃO existe na tela, nós criamos a estrutura dele
        if (!card) {
            card = document.createElement('div');
            card.id = cardId;
            card.className = 'bg-[#1b212c] p-4 rounded-lg border border-gray-800 hover:border-blue-500 transition cursor-pointer flex justify-between items-center mb-2 group shadow-sm';
            statusList.appendChild(card);
        }

        // 2. ATUALIZAÇÃO: Se o card já existe, apenas trocamos o conteúdo interno
        // Isso impede que novos cards apareçam se o ticker for o mesmo
        card.onclick = () => fetchTickerHistory(item.ticker);
        
        card.innerHTML = `
            <div>
                <span class="block font-bold text-white group-hover:text-blue-400 transition">${item.ticker}</span>
                <span class="text-[10px] text-gray-500 uppercase font-mono tracking-tighter">Gold Layer</span>
            </div>
            <div class="text-right">
                <span class="block font-mono text-sm text-blue-300">R$ ${item.preco_fechamento.toFixed(2)}</span>
                <span class="text-[10px] font-bold text-gray-400">NEUTRO</span>
            </div>
        `;
    });
}

// Ingestão protegida
async function requestIngestion() {
    const tickerInput = document.getElementById('new-ticker');
    const ticker = tickerInput.value.trim().toUpperCase();
    
    if(!ticker) return showAlert("Digite um ticker!", true);
    
    showAlert(`Iniciando processamento de ${ticker}...`);

    try {
        const response = await fetch(`${API_URL}/ingest/${ticker}`, { method: 'POST' });
        const result = await response.json();

        if (result.status === "success") {
            showAlert(result.message);
            // Atualiza a lista lateral para o novo ativo aparecer sem duplicar
            fetchMarketStatus();
        } else {
            showAlert(result.message, true);
        }
    } catch (error) {
        showAlert("Erro de conexão com o servidor.", true);
    }
}

// --- Funções de Gráfico e Histórico (Mantidas e Otimizadas) ---

async function fetchTickerHistory(ticker) {
    console.log("📈 Buscando histórico de:", ticker);
    const tickerTitle = document.getElementById('current-ticker');
    if (tickerTitle) tickerTitle.innerText = `Carregando ${ticker}...`;
    
    try {
        const response = await fetch(`${API_URL}/history/${ticker}`);
        const data = await response.json();
        
        if (!data || data.length === 0) {
            showAlert("Nenhum dado encontrado para este ativo.", true);
            return;
        }

        updateChart(ticker, data);
        if (tickerTitle) tickerTitle.innerText = `Análise Técnica: ${ticker}`;
    } catch (error) {
        console.error("Erro ao buscar histórico:", error);
    }
}

function updateChart(ticker, history) {
    const chartCanvas = document.getElementById('mainChart');
    if (!chartCanvas) return;
    const ctx = chartCanvas.getContext('2d');
    
    // Pegamos os últimos 60 dias
    const recent = history.slice(-60);

    // O PULO DO GATO: Mapeamento "Blindado" para as colunas da Gold Layer
    const labels = recent.map(h => h.data_referencia || h.DATA_REFERENCIA || h.date);
    const prices = recent.map(h => h.preco_fechamento || h.PRECO_FECHAMENTO || h.close);
    
    // Tentamos todas as variações possíveis que o dbt/Spark costumam gerar
    const b_sup = recent.map(h => h.bband_superior || h.BBAND_SUPERIOR || h.upper_band || h.UPPER_BAND);
    const b_inf = recent.map(h => h.bband_inferior || h.BBAND_INFERIOR || h.lower_band || h.LOWER_BAND);

    console.log("Amostra de Bandas (Superior):", b_sup[0]);

    // Cálculos de escala para o gráfico não "sumir" nas bordas
    const minPrice = Math.min(...prices) * 0.98;
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
                    zIndex: 10
                },
                {
                    label: 'Banda Sup',
                    data: b_sup,
                    borderColor: 'rgba(59, 130, 246, 0.2)', // Azul sutil na borda
                    borderWidth: 1,
                    pointRadius: 0,
                    fill: false,
                    tension: 0.1
                },
                {
                    label: 'Banda Inf',
                    data: b_inf,
                    borderColor: 'rgba(59, 130, 246, 0.2)',
                    borderWidth: 1,
                    pointRadius: 0,
                    // AZUL "TRADINGVIEW": Um preenchimento leve que dá profundidade
                    backgroundColor: 'rgba(59, 130, 246, 0.08)', 
                    fill: '-1', 
                    tension: 0.1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    position: 'right',
                    min: minPrice,
                    max: maxPrice,
                    ticks: { color: '#848e9c' },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                x: {
                    ticks: { color: '#848e9c', maxTicksLimit: 8 },
                    grid: { display: false }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });
}

// Funções de Alerta
function showAlert(message, isError = false) {
    const alertDiv = document.getElementById('custom-alert');
    const msgSpan = document.getElementById('alert-message');
    if (!alertDiv || !msgSpan) return;
    
    alertDiv.style.borderLeftColor = isError ? "#f6465d" : "#0ecb81";
    msgSpan.innerText = message;
    alertDiv.classList.remove('hidden');
    setTimeout(closeAlert, 5000);
}

function closeAlert() {
    const alertDiv = document.getElementById('custom-alert');
    if (alertDiv) alertDiv.classList.add('hidden');
}

// --- INICIALIZAÇÃO CONTROLADA ---
window.onload = () => {
    if (marketInterval) clearInterval(marketInterval);
    
    fetchMarketStatus();
    // Atualiza a lateral a cada 10 segundos (suficiente para o terminal)
    marketInterval = setInterval(fetchMarketStatus, 10000);

    // Se houver um ticker na URL, já carrega o gráfico dele
    const urlParams = new URLSearchParams(window.location.search);
    const tickerParam = urlParams.get('ticker');
    if (tickerParam) {
        fetchTickerHistory(tickerParam.toUpperCase());
    }
};