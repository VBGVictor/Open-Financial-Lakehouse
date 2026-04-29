// 1. Definição da URL da API
const API_URL = "http://127.0.0.1:8000";

// Aumente a frequência para o vídeo
setInterval(fetchLogs, 1000); 

async function fetchLogs() {
    try {
        const response = await fetch(`${API_URL}/logs?t=${Date.now()}`);
        const data = await response.json();
        const logContent = document.getElementById('log-content');
        
        if (logContent && data.logs) {
            // Atualiza o texto sem resetar a posição do scroll bruscamente
            logContent.innerText = data.logs.join('\n');
            
            // const container = document.getElementById('log-container');
            // container.scrollTop = container.scrollHeight; 
        }
    } catch (error) {
        console.error("Erro ao sincronizar logs:", error);
    }
}

// 2. Funções de Alerta
function closeAlert() {
    const alertDiv = document.getElementById('custom-alert');
    if (alertDiv) alertDiv.classList.add('hidden');
}

function showAlert(message, isError = false) {
    const alertDiv = document.getElementById('custom-alert');
    const msgSpan = document.getElementById('alert-message');
    
    if (!alertDiv || !msgSpan) return;

    alertDiv.style.borderLeft = isError ? "5px solid #f6465d" : "5px solid #0ecb81";
    msgSpan.innerText = message;
    alertDiv.classList.remove('hidden');

    setTimeout(closeAlert, 6000);
}

// 3. Ingestão
async function requestIngestion() {
    const tickerInput = document.getElementById('new-ticker');
    const ticker = tickerInput.value.trim().toUpperCase();
    
    if(!ticker) return showAlert("Digite um ticker!", true);
    
    showAlert(`Iniciando processamento de ${ticker}...`);

    // Feedback imediato no monitor
    const logContent = document.getElementById('log-content');
    if (logContent) logContent.innerText = `> Solicitando processamento de ${ticker}...\n`;

    // O PULO DO GATO: Não usamos "await" na frente do fetch de ingestão 
    // para que a função não trave o resto do script.
    fetch(`${API_URL}/ingest/${ticker}`, { method: 'POST' })
        .then(response => response.json())
        .then(result => {
            if (result.status === "success") {
                showAlert(result.message);
                fetchMarketStatus();
            } else {
                showAlert(result.message, true);
            }
        })
        .catch(error => {
            showAlert("Erro de conexão.", true);
        });

    // O script continua aqui sem esperar a resposta acima, 
    // permitindo que o setInterval(fetchLogs) continue rodando livremente!
}

// 4. Buscar Status do Mercado
async function fetchMarketStatus() {
    try {
        const response = await fetch(`${API_URL}/market-status`);
        const data = await response.json();
        
        const statusList = document.getElementById('status-list');
        if (!statusList) return;

        statusList.innerHTML = ''; 

        // O PULO DO GATO: Filtrar duplicados mantendo apenas o último registro de cada ticker
        const uniqueAssets = Array.from(
            data.reduce((map, item) => map.set(item.ticker, item), new Map()).values()
        );

        uniqueAssets.forEach(item => {
            const card = document.createElement('div');
            // Mantendo o estilo de card do terminal
            card.className = 'bg-[#1b212c] p-4 rounded-lg border border-gray-800 hover:border-blue-500 transition cursor-pointer group shadow-sm flex justify-between items-center';
            
            card.onclick = () => {
                if (typeof loadTickerData === "function") {
                    loadTickerData(item.ticker);
                } else {
                    window.location.href = `index.html?ticker=${item.ticker}`;
                }
            };
            
            card.innerHTML = `
                <div>
                    <span class="block font-bold text-white group-hover:text-blue-400 transition">${item.ticker}</span>
                    <span class="text-[10px] text-gray-500 uppercase font-mono">Status: Neutro</span>
                </div>
                <div class="text-right">
                    <span class="block font-mono text-sm text-blue-300">R$ ${item.preco_fechamento.toFixed(2)}</span>
                </div>
            `;
            statusList.appendChild(card);
        });
    } catch (error) {
        console.error("Erro ao carregar Market Status:", error);
    }
}

// 5. Inicialização Robusta
window.onload = () => {
    console.log("🚀 [SISTEMA] Iniciando componentes...");

    // 1. Carrega os ativos iniciais
    fetchMarketStatus();

    // 2. O PULO DO GATO: Ligar o monitor de logs de forma isolada
    // Usamos uma função anônima para garantir que o escopo seja respeitado
    const motorDeLogs = setInterval(function() {
        console.log("📡 [MONITOR] Verificando logs...");
        fetchLogs();
    }, 2000);

    // Guardamos o ID do motor para caso queira parar depois
    window.logTimer = motorDeLogs;
};