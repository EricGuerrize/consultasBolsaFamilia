import React, { useState, useEffect } from 'react';
import { Upload, Search, Download, AlertCircle, CheckCircle2, ShieldCheck, LayoutDashboard, History, Settings } from 'lucide-react';

function App() {
  const [file, setFile] = useState(null);
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [jobId, setJobId] = useState(null);
  const [status, setStatus] = useState(null);
  const [config, setConfig] = useState({
    m_ini: '202401',
    m_fim: '202403',
    modo: 'municipio',
    ibge: '',
    api_key: '',
    col_cpf: 'cpf',
    col_nome: 'nome'
  });

  // Polling para o status do cruzamento
  useEffect(() => {
    let interval;
    if (jobId && status?.status === 'processing') {
      interval = setInterval(async () => {
        try {
          const res = await fetch(`/api/status/${jobId}`);
          const data = await res.json();
          setStatus(data);
          if (data.status === 'completed' || data.status === 'failed') {
            clearInterval(interval);
            setLoading(false);
          }
        } catch (err) {
          console.error("Erro ao checar status", err);
        }
      }, 2000);
    }
    return () => clearInterval(interval);
  }, [jobId, status]);

  const handleFileUpload = async (e) => {
    const uploadedFile = e.target.files[0];
    if (!uploadedFile) return;

    setFile(uploadedFile);
    const formData = new FormData();
    formData.append('file', uploadedFile);

    try {
      const res = await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      });
      const data = await res.json();
      setColumns(data.columns);
    } catch (err) {
      alert("Erro ao ler colunas do arquivo.");
    }
  };

  const startCrossing = async () => {
    if (!file) return;
    setLoading(true);
    
    const formData = new FormData();
    formData.append('file', file);
    Object.keys(config).forEach(key => formData.append(key, config[key]));

    try {
      const res = await fetch('/api/cross', {
        method: 'POST',
        body: formData,
      });
      const data = await res.json();
      setJobId(data.job_id);
      setStatus({ status: 'processing', progress: 0 });
    } catch (err) {
      setLoading(false);
      alert("Erro ao iniciar cruzamento.");
    }
  };

  const exportResults = () => {
    if (!status?.result) return;
    const csvContent = "data:text/csv;charset=utf-8," 
      + ["Servidor", "CPF", "Beneficiario", "Municipio", "UF", "Mes", "Valor"].join(",") + "\n"
      + status.result.map(r => Object.values(r).join(",")).join("\n");
    
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", "resultados_bolsafamilia.csv");
    document.body.appendChild(link);
    link.click();
  };

  return (
    <div className="container animate-in">
      <header>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <ShieldCheck size={32} color="#3b82f6" />
          <h1>Portal de Auditoria Bolsa Família</h1>
        </div>
        <div style={{ display: 'flex', gap: '1rem' }}>
          <button className="btn" style={{ background: 'transparent' }}>
            <History size={18} style={{ marginRight: '8px' }} /> Histórico
          </button>
          <button className="btn" style={{ background: 'transparent' }}>
            <Settings size={18} style={{ marginRight: '8px' }} /> Config
          </button>
        </div>
      </header>

      {/* Stats Quick View */}
      <div className="dashboard-grid">
        <div className="card glass">
          <div className="card-title">Status da API</div>
          <div className="card-value" style={{ color: '#10b981', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <CheckCircle2 size={24} /> Conectado
          </div>
        </div>
        <div className="card glass">
          <div className="card-title">Servidores Carregados</div>
          <div className="card-value">{file ? "Confirmado" : "---"}</div>
        </div>
        <div className="card glass">
          <div className="card-title">Alertas Encontrados</div>
          <div className="card-value" style={{ color: status?.result?.length > 0 ? '#ef4444' : 'inherit' }}>
            {status?.result ? status.result.length : "0"}
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '2rem' }}>
        {/* Sidebar de Configuração */}
        <div className="form-section glass">
          <h2 style={{ fontSize: '1.2rem', marginBottom: '1.5rem' }}>Configuração</h2>
          
          <div className="input-group">
            <label>Arquivo de Servidores</label>
            <div style={{ 
              border: '2px dashed var(--border-color)', 
              borderRadius: '8px', 
              padding: '1.5rem', 
              textAlign: 'center',
              cursor: 'pointer'
            }} onClick={() => document.getElementById('fileInput').click()}>
              <Upload size={24} style={{ marginBottom: '8px', color: 'var(--text-secondary)' }} />
              <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                {file ? file.name : "Clique para subir CSV ou Excel"}
              </div>
              <input 
                id="fileInput" 
                type="file" 
                hidden 
                onChange={handleFileUpload}
                accept=".csv,.xlsx,.xls"
              />
            </div>
          </div>

          <div className="input-group">
            <label>Chave de API (Portal da Transparência)</label>
            <input 
              type="password" 
              placeholder="••••••••••••••••" 
              value={config.api_key}
              onChange={e => setConfig({...config, api_key: e.target.value})}
            />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
            <div className="input-group">
              <label>Mês Início</label>
              <input type="text" placeholder="202401" value={config.m_ini} onChange={e => setConfig({...config, m_ini: e.target.value})} />
            </div>
            <div className="input-group">
              <label>Mês Fim</label>
              <input type="text" placeholder="202403" value={config.m_fim} onChange={e => setConfig({...config, m_fim: e.target.value})} />
            </div>
          </div>

          <div className="input-group">
            <label>Modo de Cruzamento</label>
            <select value={config.modo} onChange={e => setConfig({...config, modo: e.target.value})}>
              <option value="municipio">Em Lote (Por Município)</option>
              <option value="cpf">Pincelado (Por CPF)</option>
            </select>
          </div>

          {config.modo === 'municipio' && (
            <div className="input-group">
              <label>Código IBGE (Município)</label>
              <input type="text" placeholder="Ex: 5107602" value={config.ibge} onChange={e => setConfig({...config, ibge: e.target.value})} />
            </div>
          )}

          <button 
            className="btn btn-primary" 
            style={{ width: '100%', marginTop: '1rem' }}
            disabled={!file || loading}
            onClick={startCrossing}
          >
            {loading ? "Processando..." : <><Search size={18} style={{ marginRight: '8px' }} /> Iniciar Cruzamento</>}
          </button>
        </div>

        {/* Área de Resultados */}
        <div className="glass" style={{ padding: '2rem', borderRadius: 'var(--radius)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
            <h2 style={{ fontSize: '1.2rem' }}>Resultados do Cruzamento</h2>
            {status?.result?.length > 0 && (
              <button className="btn btn-primary" style={{ padding: '0.5rem 1rem' }} onClick={exportResults}>
                <Download size={16} style={{ marginRight: '6px' }} /> Exportar
              </button>
            )}
          </div>

          {loading && (
            <div style={{ textAlign: 'center', padding: '3rem' }}>
              <div className="progress-bar-bg">
                <div className="progress-bar-fill" style={{ width: `${status?.progress || 0}%` }}></div>
              </div>
              <div style={{ color: 'var(--text-secondary)', marginTop: '0.5rem' }}>
                Progresso: {status?.progress || 0}%
              </div>
            </div>
          )}

          {!loading && (!status?.result || status.result.length === 0) && (
            <div style={{ textAlign: 'center', padding: '5rem', color: 'var(--text-dim)' }}>
              <LayoutDashboard size={48} style={{ marginBottom: '1rem', opacity: 0.2 }} />
              <p>Nenhum dado processado ainda.</p>
              <p style={{ fontSize: '0.8rem' }}>Ajuste os filtros e clique em Iniciar.</p>
            </div>
          )}

          {status?.result?.length > 0 && (
            <div className="results-container">
              <table>
                <thead>
                  <tr>
                    <th>Servidor</th>
                    <th>CPF</th>
                    <th>Beneficiário</th>
                    <th>Município</th>
                    <th>Mês</th>
                    <th>Valor</th>
                  </tr>
                </thead>
                <tbody>
                  {status.result.map((row, idx) => (
                    <tr key={idx}>
                      <td>{row.servidor}</td>
                      <td>{row.cpf}</td>
                      <td>{row.beneficiario}</td>
                      <td>{row.municipio} / {row.uf}</td>
                      <td>{row.mes}</td>
                      <td style={{ fontWeight: 600, color: '#f87171' }}>R$ {row.valor.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
