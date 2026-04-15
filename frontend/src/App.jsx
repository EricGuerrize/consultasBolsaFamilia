import React, { useState, useEffect, useRef } from 'react';
import {
  Upload, Search, Download, AlertCircle, CheckCircle2,
  ShieldCheck, XCircle, Loader2, Filter, FileText
} from 'lucide-react';

export default function App() {
  const [file, setFile] = useState(null);
  const [columns, setColumns] = useState([]);
  const [fileInfo, setFileInfo] = useState(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState(null);
  const [apiHealth, setApiHealth] = useState('checking');
  const [error, setError] = useState(null);
  const [searchFilter, setSearchFilter] = useState('');
  const [municipios, setMunicipios] = useState([]);
  const [municipioSearch, setMunicipioSearch] = useState('');
  const [showMunicipioDropdown, setShowMunicipioDropdown] = useState(false);
  const municipioRef = useRef(null);
  const [modoTeste, setModoTeste] = useState(true);
  const [config, setConfig] = useState({
    m_ini: '202401',
    m_fim: '202403',
    modo: 'municipio',
    ibge: '',
    api_key: '',
    col_cpf: '',
    col_nome: '',
  });

  useEffect(() => {
    fetch('/api/health')
      .then(r => (r.ok ? setApiHealth('ok') : setApiHealth('error')))
      .catch(() => setApiHealth('error'));
  }, []);

  useEffect(() => {
    fetch('https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome')
      .then(r => r.json())
      .then(data => {
        setMunicipios(data.map(m => ({
          id: String(m.id),
          nome: m.nome,
          uf: m.microrregiao?.mesorregiao?.UF?.sigla || '',
        })));
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (municipioRef.current && !municipioRef.current.contains(e.target)) {
        setShowMunicipioDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const parseCSV = (file) => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const text = e.target.result;
          const rows = text.split(/\r?\n/).filter(line => line.trim());
          if (rows.length === 0) throw new Error('Arquivo vazio');
          
          // Detecta delimitador (vírgula ou ponto-e-vírgula)
          const firstLine = rows[0];
          const delims = [',', ';', '\t'];
          const counts = delims.map(d => firstLine.split(d).length);
          const maxIdx = counts.indexOf(Math.max(...counts));
          const sep = delims[maxIdx];

          const headers = firstLine.split(sep).map(h => h.trim().replace(/^"|"$/g, ''));
          resolve({ headers, sep, total: rows.length - 1, rows });
        } catch (err) {
          reject(err);
        }
      };
      reader.onerror = () => reject(new Error('Erro ao ler arquivo'));
      reader.readAsText(file);
    });
  };

  const handleFileUpload = async (e) => {
    const uploadedFile = e.target.files[0];
    if (!uploadedFile) return;

    const MAX_SIZE = 10 * 1024 * 1024; // 10 MB
    if (uploadedFile.size > MAX_SIZE) {
      setError(`Arquivo muito grande (${(uploadedFile.size / 1024 / 1024).toFixed(1)} MB). O limite é 10 MB.`);
      return;
    }

    setFile(uploadedFile);
    setColumns([]);
    setFileInfo(null);
    setError(null);
    setStatus(null);

    const suffix = uploadedFile.name.split('.').pop().toLowerCase();
    if (suffix === 'csv') {
      try {
        const { headers, total } = await parseCSV(uploadedFile);
        setColumns(headers);
        setFileInfo({ total, filename: uploadedFile.name });
        const cpfCol = headers.find(c => /cpf/i.test(c)) || '';
        const nomeCol = headers.find(c => /^nome|name|servidor/i.test(c)) || '';
        setConfig(prev => ({ ...prev, col_cpf: cpfCol, col_nome: nomeCol }));
      } catch (err) {
        setError("Erro ao ler CSV: " + err.message);
      }
    } else {
      // Para Excel ainda usamos o backend pra ler colunas (ou o user converte pra CSV)
      const formData = new FormData();
      formData.append('file', uploadedFile);
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: formData });
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(txt.includes('Request Entity Too Large') ? 'Arquivo muito grande. Tente usar formato CSV.' : 'Erro ao ler arquivo');
        }
        const data = await res.json();
        setColumns(data.columns);
        setFileInfo({ total: data.total, filename: data.filename });
        const cpfCol = data.columns.find(c => /cpf/i.test(c)) || '';
        const nomeCol = data.columns.find(c => /^nome|name|servidor/i.test(c)) || '';
        setConfig(prev => ({ ...prev, col_cpf: cpfCol, col_nome: nomeCol }));
      } catch (err) {
        setError(err.message);
      }
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    const dropped = e.dataTransfer.files[0];
    if (dropped) handleFileUpload({ target: { files: [dropped] } });
  };

  // ── Lógica de cruzamento (executada no browser) ──────────────────
  const normalizarCPF = (cpf) => {
    if (!cpf) return '';
    const d = String(cpf).replace(/\D/g, '');
    return d.length <= 11 ? d.padStart(11, '0').slice(0, 11) : d.slice(0, 11);
  };

  const meioCPF = (cpfRaw) => {
    const raw = String(cpfRaw || '');
    const full = raw.replace(/\D/g, '');
    if (full.length === 11) return full.slice(3, 9);
    const masked = raw.replace(/[xX*]/g, '').replace(/\D/g, '');
    return masked.slice(0, 6);
  };

  const primeiroNome = (nome) =>
    nome ? String(nome).trim().split(/\s+/)[0].toUpperCase() : '';

  const chaveJS = (cpfRaw, nome) => {
    const meio = meioCPF(cpfRaw);
    const pnome = primeiroNome(nome);
    return meio && pnome ? `${meio}|${pnome}` : '';
  };

  const formatResultJS = (srv, reg) => {
    const bf  = reg.beneficiarioNovoBolsaFamilia || {};
    const mun = reg.municipio || {};
    const uf  = mun.uf || {};
    const sigla = String(uf.sigla || uf.nome || '').slice(0, 2);
    const mesRaw = (reg.dataMesReferencia || reg.mesReferencia || '').replace(/-/g, '').slice(0, 6);
    return {
      servidor:    srv.nome || '',
      cpf:         srv.cpf  || '',
      beneficiario: bf.nome || '',
      municipio:   mun.nomeIBGE || '',
      uf:          sigla,
      mes:         mesRaw,
      data_saque:  reg.dataSaque || '',
      valor:       reg.valorSaque ?? reg.valor ?? 0,
    };
  };

  const getMesesList = (ini, fim) => {
    const meses = [];
    let [y, m] = [parseInt(ini.slice(0, 4)), parseInt(ini.slice(4))];
    const [ey, em] = [parseInt(fim.slice(0, 4)), parseInt(fim.slice(4))];
    while (y < ey || (y === ey && m <= em)) {
      meses.push(`${y}${String(m).padStart(2, '0')}`);
      m++; if (m > 12) { m = 1; y++; }
    }
    return meses;
  };

  const proxyFetch = async (endpoint, params, retries = 3) => {
    const res = await fetch('/api/proxy', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint, params, api_key: config.api_key }),
    });
    if (res.status === 429) { await new Promise(r => setTimeout(r, 1500)); return proxyFetch(endpoint, params, retries); }
    if (res.status === 504 && retries > 0) { await new Promise(r => setTimeout(r, 3000)); return proxyFetch(endpoint, params, retries - 1); }
    if (!res.ok) { const t = await res.text(); let msg = t; try { msg = JSON.parse(t)?.detail || t; } catch {} throw new Error(msg); }
    return res.json();
  };

  const startCrossing = async () => {
    if (!file) return;
    setLoading(true);
    setError(null);
    setStatus(null);
    setSearchFilter('');

    try {
      const { rows, sep, headers } = await parseCSV(file);
      const cpfIdx  = headers.indexOf(config.col_cpf);
      const nomeIdx = headers.indexOf(config.col_nome);

      // Normaliza CPFs e monta mapa de cruzamento: chave → [servidor]
      const serverMap = new Map();
      rows.slice(1).forEach(row => {
        const cells = row.split(sep);
        const cpf  = normalizarCPF(cells[cpfIdx]?.replace(/^"|"$/g, ''));
        const nome = nomeIdx !== -1 ? (cells[nomeIdx]?.replace(/^"|"$/g, '') || '') : '';
        if (!cpf) return;
        const chave = chaveJS(cpf, nome);
        if (chave) {
          if (!serverMap.has(chave)) serverMap.set(chave, []);
          serverMap.get(chave).push({ cpf, nome });
        }
      });

      const meses = getMesesList(config.m_ini, config.m_fim);
      const allResults = [];

      for (let i = 0; i < meses.length; i++) {
        const mes = meses[i];

        if (config.modo === 'municipio') {
          let pagina = 1;
          const MAX_PAGINAS = modoTeste ? 100 : Infinity;
          while (true) {
            setStatus({
              status: 'processing',
              progress: Math.round((i / meses.length) * 100),
              message: `${mes} — página ${pagina}${modoTeste ? ' (modo teste)' : ''}…`,
            });
            const regs = await proxyFetch('municipio', { mesAno: mes, codigoIbge: config.ibge, pagina });
            for (const reg of regs) {
              const bf = reg.beneficiarioNovoBolsaFamilia || {};
              const chave = chaveJS(bf.cpfFormatado || '', bf.nome || '');
              if (chave && serverMap.has(chave)) {
                for (const srv of serverMap.get(chave)) allResults.push(formatResultJS(srv, reg));
              }
            }
            if (regs.length < 15 || pagina >= MAX_PAGINAS) break;
            pagina++;
            await new Promise(r => setTimeout(r, 150));
          }
        } else {
          // Modo CPF: 1 request por servidor
          const todosServidores = [...serverMap.values()].flat();
          const servidores = modoTeste ? todosServidores.slice(0, 500) : todosServidores;
          for (let j = 0; j < servidores.length; j++) {
            const srv = servidores[j];
            setStatus({
              status: 'processing',
              progress: Math.round(((i * servidores.length + j) / (meses.length * servidores.length)) * 100),
              message: `${mes} — CPF ${j + 1}/${servidores.length}…`,
            });
            const regs = await proxyFetch('cpf', { cpf: srv.cpf, pagina: 1 });
            for (const reg of regs) {
              const mesRef = (reg.mesReferencia || '').replace(/-/g, '').slice(0, 6);
              if (mesRef !== mes) continue;
              const bf = reg.beneficiarioNovoBolsaFamilia || {};
              if (chaveJS(bf.cpfFormatado || '', bf.nome || '') === chaveJS(srv.cpf, srv.nome))
                allResults.push(formatResultJS(srv, reg));
            }
            await new Promise(r => setTimeout(r, 100));
          }
        }
      }

      setStatus({ status: 'completed', progress: 100, result: allResults });
      setLoading(false);
    } catch (err) {
      setLoading(false);
      setError(err.message);
    }
  };

  const filteredResults = (status?.result || []).filter(r => {
    if (!searchFilter) return true;
    const q = searchFilter.toLowerCase();
    return (
      r.servidor?.toLowerCase().includes(q) ||
      r.cpf?.includes(q) ||
      r.municipio?.toLowerCase().includes(q) ||
      r.beneficiario?.toLowerCase().includes(q)
    );
  });

  const totalValue = (status?.result || []).reduce((sum, r) => sum + (r.valor || 0), 0);

  const exportCSV = () => {
    if (!status?.result) return;
    const headers = ['Servidor', 'CPF', 'Beneficiário', 'Município', 'UF', 'Mês Ref.', 'Data Saque', 'Valor'];
    const escape = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const rows = status.result.map(r =>
      [r.servidor, r.cpf, r.beneficiario, r.municipio, r.uf, r.mes, r.data_saque, r.valor].map(escape)
    );
    const csv = '\uFEFF' + [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'resultados_bolsafamilia.csv';
    link.click();
    URL.revokeObjectURL(url);
  };

  const canStart = file && !loading && config.col_cpf;

  return (
    <div className="container animate-in">
      <header>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <ShieldCheck size={30} color="#3b82f6" />
          <h1>Portal de Auditoria Bolsa Família</h1>
        </div>
      </header>

      {/* Stats */}
      <div className="dashboard-grid">
        <div className="card glass">
          <div className="card-title">Status da API</div>
          {apiHealth === 'checking' && (
            <div className="card-value status-checking">
              <Loader2 size={18} className="spin" /> Verificando...
            </div>
          )}
          {apiHealth === 'ok' && (
            <div className="card-value status-ok">
              <CheckCircle2 size={22} /> Conectado
            </div>
          )}
          {apiHealth === 'error' && (
            <div className="card-value status-error">
              <XCircle size={22} /> Offline
            </div>
          )}
        </div>

        <div className="card glass">
          <div className="card-title">Servidores Carregados</div>
          <div className="card-value">{fileInfo ? fileInfo.total.toLocaleString('pt-BR') : '---'}</div>
          {fileInfo && (
            <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '4px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {fileInfo.filename}
            </div>
          )}
        </div>

        <div className="card glass">
          <div className="card-title">Alertas Encontrados</div>
          <div className="card-value" style={{ color: (status?.result?.length || 0) > 0 ? 'var(--error)' : 'inherit' }}>
            {status?.result ? status.result.length.toLocaleString('pt-BR') : '0'}
          </div>
          {(status?.result?.length || 0) > 0 && (
            <div style={{ fontSize: '0.72rem', color: 'var(--error)', marginTop: '4px' }}>
              Total: R$ {totalValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
            </div>
          )}
        </div>
      </div>

      {error && (
        <div className="error-banner">
          <AlertCircle size={16} />
          <span>{error}</span>
        </div>
      )}

      <div className="main-grid">
        {/* Sidebar */}
        <div className="form-section glass">
          <h2 className="section-title">Configuração</h2>

          <div className="input-group">
            <label>Arquivo de Servidores</label>
            <div
              className={`upload-zone${file ? ' uploaded' : ''}`}
              onClick={() => document.getElementById('fileInput').click()}
              onDragOver={e => e.preventDefault()}
              onDrop={handleDrop}
            >
              <Upload size={22} style={{ color: file ? 'var(--success)' : 'var(--text-dim)' }} />
              <div className="upload-label">
                {file ? file.name : 'Arraste ou clique para subir CSV / Excel'}
              </div>
              {fileInfo && <div className="badge-success">{fileInfo.total.toLocaleString('pt-BR')} registros</div>}
              <input id="fileInput" type="file" hidden onChange={handleFileUpload} accept=".csv,.xlsx,.xls" />
            </div>
          </div>

          {columns.length > 0 && (
            <div className="mapping-box">
              <div className="mapping-title">Mapeamento de Colunas</div>
              <div className="input-group" style={{ marginBottom: '0.75rem' }}>
                <label>Coluna de CPF *</label>
                <select value={config.col_cpf} onChange={e => setConfig({ ...config, col_cpf: e.target.value })}>
                  <option value="">-- Selecione --</option>
                  {columns.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
              <div className="input-group" style={{ marginBottom: 0 }}>
                <label>Coluna de Nome</label>
                <select value={config.col_nome} onChange={e => setConfig({ ...config, col_nome: e.target.value })}>
                  <option value="">-- Selecione --</option>
                  {columns.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
            </div>
          )}

          <div className="input-group">
            <label>Chave de API (Opcional - lida do servidor se vazia)</label>
            <input
              type="password"
              placeholder="Chave do Portal da Transparência"
              value={config.api_key}
              onChange={e => setConfig({ ...config, api_key: e.target.value })}
            />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
            <div className="input-group">
              <label>Mês Início</label>
              <input
                type="text"
                placeholder="YYYYMM"
                value={config.m_ini}
                maxLength={6}
                onChange={e => setConfig({ ...config, m_ini: e.target.value })}
              />
            </div>
            <div className="input-group">
              <label>Mês Fim</label>
              <input
                type="text"
                placeholder="YYYYMM"
                value={config.m_fim}
                maxLength={6}
                onChange={e => setConfig({ ...config, m_fim: e.target.value })}
              />
            </div>
          </div>

          <div className="input-group">
            <label>Modo de Cruzamento</label>
            <select value={config.modo} onChange={e => setConfig({ ...config, modo: e.target.value })}>
              <option value="municipio">Em Lote (Por Município)</option>
              <option value="cpf">Individual (Por CPF)</option>
            </select>
          </div>

          {config.modo === 'municipio' && (
            <div className="input-group" ref={municipioRef}>
              <label>Município</label>
              <div className="municipio-combobox">
                <input
                  type="text"
                  placeholder={municipios.length === 0 ? 'Carregando municípios...' : 'Buscar município...'}
                  value={municipioSearch}
                  disabled={municipios.length === 0}
                  onChange={e => {
                    setMunicipioSearch(e.target.value);
                    setShowMunicipioDropdown(true);
                    if (!e.target.value) setConfig(prev => ({ ...prev, ibge: '' }));
                  }}
                  onFocus={() => setShowMunicipioDropdown(true)}
                />
                {config.ibge && (
                  <span className="municipio-code-badge">{config.ibge}</span>
                )}
                {showMunicipioDropdown && municipioSearch.trim().length >= 2 && (
                  <ul className="municipio-dropdown">
                    {municipios
                      .filter(m =>
                        m.nome.toLowerCase().includes(municipioSearch.toLowerCase()) ||
                        m.uf.toLowerCase().includes(municipioSearch.toLowerCase()) ||
                        m.id.includes(municipioSearch)
                      )
                      .slice(0, 50)
                      .map(m => (
                        <li
                          key={m.id}
                          className={config.ibge === m.id ? 'selected' : ''}
                          onMouseDown={() => {
                            setConfig(prev => ({ ...prev, ibge: m.id }));
                            setMunicipioSearch(`${m.nome} - ${m.uf}`);
                            setShowMunicipioDropdown(false);
                          }}
                        >
                          <span className="municipio-nome">{m.nome}</span>
                          <span className="municipio-meta">{m.uf} · {m.id}</span>
                        </li>
                      ))}
                    {municipios.filter(m =>
                      m.nome.toLowerCase().includes(municipioSearch.toLowerCase()) ||
                      m.uf.toLowerCase().includes(municipioSearch.toLowerCase()) ||
                      m.id.includes(municipioSearch)
                    ).length === 0 && (
                      <li className="municipio-empty">Nenhum município encontrado</li>
                    )}
                  </ul>
                )}
              </div>
            </div>
          )}

          <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontSize: '0.8rem', color: modoTeste ? 'var(--warning, #f59e0b)' : 'var(--text-dim)', marginTop: '0.5rem' }}>
            <input type="checkbox" checked={modoTeste} onChange={e => setModoTeste(e.target.checked)} />
            Modo Teste {modoTeste ? '(100 págs/mês · 500 CPFs)' : '— desativado (execução completa)'}
          </label>

          <button className="btn btn-primary" style={{ width: '100%', marginTop: '0.5rem' }} disabled={!canStart} onClick={startCrossing}>
            {loading
              ? <><Loader2 size={15} className="spin" style={{ marginRight: '8px' }} />Processando...</>
              : <><Search size={15} style={{ marginRight: '8px' }} />Iniciar Cruzamento</>}
          </button>
          {file && !config.col_cpf && (
            <p style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '0.5rem', textAlign: 'center' }}>
              Selecione a coluna de CPF para continuar
            </p>
          )}
        </div>

        {/* Results */}
        <div className="glass results-panel">
          <div className="results-header">
            <h2 className="section-title" style={{ marginBottom: 0 }}>Resultados do Cruzamento</h2>
            {(status?.result?.length || 0) > 0 && (
              <button className="btn btn-primary btn-sm" onClick={exportCSV}>
                <Download size={13} style={{ marginRight: '5px' }} />Exportar CSV
              </button>
            )}
          </div>

          {(status?.result?.length || 0) > 0 && (
            <div style={{ position: 'relative', marginBottom: '1rem' }}>
              <Filter size={13} className="filter-icon" />
              <input
                type="text"
                placeholder="Filtrar por nome, CPF ou município..."
                value={searchFilter}
                onChange={e => setSearchFilter(e.target.value)}
                style={{ paddingLeft: '34px' }}
              />
            </div>
          )}

          {loading && (
            <div style={{ textAlign: 'center', padding: '3rem 1rem' }}>
              <div className="progress-bar-bg">
                <div className="progress-bar-fill" style={{ width: `${status?.progress || 0}%` }} />
              </div>
              <p style={{ color: 'var(--text-secondary)', marginTop: '0.75rem', fontSize: '0.875rem' }}>
                {status?.progress || 0}% concluído
              </p>
              <p style={{ color: 'var(--text-dim)', marginTop: '0.25rem', fontSize: '0.75rem' }}>
                {status?.message || 'Consultando API do Portal da Transparência...'}
              </p>
            </div>
          )}

          {status?.status === 'failed' && (
            <div className="error-banner">
              <AlertCircle size={16} />
              <span>Erro no cruzamento: {status.error}</span>
            </div>
          )}

          {!loading && status?.status === 'completed' && status.result.length === 0 && (
            <div className="empty-state" style={{ color: 'var(--success)' }}>
              <CheckCircle2 size={44} style={{ opacity: 0.5, marginBottom: '0.75rem' }} />
              <p style={{ fontWeight: 600 }}>Nenhum servidor encontrado como beneficiário.</p>
              <p style={{ fontSize: '0.8rem', marginTop: '0.25rem' }}>O cruzamento foi concluído sem alertas.</p>
            </div>
          )}

          {!loading && !status && (
            <div className="empty-state">
              <FileText size={44} style={{ opacity: 0.15, marginBottom: '0.75rem' }} />
              <p>Nenhum dado processado ainda.</p>
              <p style={{ fontSize: '0.8rem', marginTop: '0.25rem', color: 'var(--text-dim)' }}>
                Configure os parâmetros e clique em Iniciar.
              </p>
            </div>
          )}

          {filteredResults.length > 0 && (
            <>
              <div className="results-container">
                <table>
                  <thead>
                    <tr>
                      <th style={{ width: '36px' }}>#</th>
                      <th>Servidor</th>
                      <th>CPF</th>
                      <th>Beneficiário (API)</th>
                      <th>Município / UF</th>
                      <th>Mês Ref.</th>
                      <th>Data Saque</th>
                      <th>Valor</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredResults.map((row, idx) => (
                      <tr key={idx}>
                        <td style={{ color: 'var(--text-dim)', fontSize: '0.72rem' }}>{idx + 1}</td>
                        <td style={{ fontWeight: 500 }}>{row.servidor}</td>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.78rem', letterSpacing: '0.03em' }}>{row.cpf}</td>
                        <td>{row.beneficiario}</td>
                        <td>{row.municipio}{row.uf ? ` / ${row.uf}` : ''}</td>
                        <td>{row.mes}</td>
                        <td>{row.data_saque || '—'}</td>
                        <td className="valor-cell">R$ {(row.valor || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {searchFilter && (
                <p style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '0.5rem', textAlign: 'right' }}>
                  Exibindo {filteredResults.length} de {status.result.length} resultados
                </p>
              )}

              <div className="summary-bar">
                <span>Total de alertas: <strong>{status.result.length.toLocaleString('pt-BR')}</strong></span>
                <span>Valor total: <strong>R$ {totalValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong></span>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
