import React, { useState, useEffect, useRef, useMemo } from 'react';
import {
  Upload, Search, Download, AlertCircle, CheckCircle2,
  ShieldCheck, XCircle, Loader2, Filter, FileText,
  ChevronDown, ChevronRight, RotateCcw, Users, X,
} from 'lucide-react';

export default function App() {
  // ── State ──────────────────────────────────────────────────────────
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

  // Fase: 'config' | 'processing' | 'done'
  const [fase, setFase] = useState('config');
  const [agrupado, setAgrupado] = useState(false);
  const [expandedRows, setExpandedRows] = useState(new Set());
  const cancelRef = useRef(false);

  // ── Effects ────────────────────────────────────────────────────────
  useEffect(() => {
    fetch('/api/health')
      .then(r => (r.ok ? setApiHealth('ok') : setApiHealth('error')))
      .catch(() => setApiHealth('error'));
  }, []);

  useEffect(() => {
    fetch('https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome')
      .then(r => r.json())
      .then(data => setMunicipios(data.map(m => ({
        id: String(m.id),
        nome: m.nome,
        uf: m.microrregiao?.mesorregiao?.UF?.sigla || '',
      }))))
      .catch(() => {});
  }, []);

  useEffect(() => {
    const handler = (e) => {
      if (municipioRef.current && !municipioRef.current.contains(e.target))
        setShowMunicipioDropdown(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // ── CSV parsing ────────────────────────────────────────────────────
  const parseCSV = (f) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const rows = e.target.result.split(/\r?\n/).filter(l => l.trim());
        if (!rows.length) throw new Error('Arquivo vazio');
        const delims = [',', ';', '\t'];
        const counts = delims.map(d => rows[0].split(d).length);
        const sep = delims[counts.indexOf(Math.max(...counts))];
        const headers = rows[0].split(sep).map(h => h.trim().replace(/^"|"$/g, ''));
        resolve({ headers, sep, total: rows.length - 1, rows });
      } catch (err) { reject(err); }
    };
    reader.onerror = () => reject(new Error('Erro ao ler arquivo'));
    reader.readAsText(f);
  });

  // ── File upload ────────────────────────────────────────────────────
  const handleFileUpload = async (e) => {
    const uploadedFile = e.target.files[0];
    if (!uploadedFile) return;
    if (uploadedFile.size > 10 * 1024 * 1024) {
      setError(`Arquivo muito grande (${(uploadedFile.size / 1024 / 1024).toFixed(1)} MB). Limite: 10 MB.`);
      return;
    }
    setFile(uploadedFile); setColumns([]); setFileInfo(null); setError(null); setStatus(null);
    const suffix = uploadedFile.name.split('.').pop().toLowerCase();
    if (suffix === 'csv') {
      try {
        const { headers, total } = await parseCSV(uploadedFile);
        setColumns(headers);
        setFileInfo({ total, filename: uploadedFile.name });
        setConfig(prev => ({
          ...prev,
          col_cpf: headers.find(c => /cpf/i.test(c)) || '',
          col_nome: headers.find(c => /^nome|name|servidor/i.test(c)) || '',
        }));
      } catch (err) { setError('Erro ao ler CSV: ' + err.message); }
    } else {
      const formData = new FormData();
      formData.append('file', uploadedFile);
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: formData });
        if (!res.ok) throw new Error('Erro ao ler arquivo');
        const data = await res.json();
        setColumns(data.columns);
        setFileInfo({ total: data.total, filename: data.filename });
        setConfig(prev => ({
          ...prev,
          col_cpf: data.columns.find(c => /cpf/i.test(c)) || '',
          col_nome: data.columns.find(c => /^nome|name|servidor/i.test(c)) || '',
        }));
      } catch (err) { setError(err.message); }
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    const f = e.dataTransfer.files[0];
    if (f) handleFileUpload({ target: { files: [f] } });
  };

  // ── Matching helpers ───────────────────────────────────────────────
  const normalizarCPF = (cpf) => {
    if (!cpf) return '';
    const d = String(cpf).replace(/\D/g, '');
    return d.length <= 11 ? d.padStart(11, '0').slice(0, 11) : d.slice(0, 11);
  };
  const meioCPF = (cpfRaw) => {
    const raw = String(cpfRaw || '');
    const full = raw.replace(/\D/g, '');
    if (full.length === 11) return full.slice(3, 9);
    return raw.replace(/[xX*]/g, '').replace(/\D/g, '').slice(0, 6);
  };
  const primeiroNome = (nome) => nome ? String(nome).trim().split(/\s+/)[0].toUpperCase() : '';
  const chaveJS = (cpfRaw, nome) => {
    const meio = meioCPF(cpfRaw), pnome = primeiroNome(nome);
    return meio && pnome ? `${meio}|${pnome}` : '';
  };
  const formatResultJS = (srv, reg) => {
    const bf = reg.beneficiarioNovoBolsaFamilia || {};
    const mun = reg.municipio || {}, uf = mun.uf || {};
    const mesRaw = (reg.dataMesReferencia || reg.mesReferencia || '').replace(/-/g, '').slice(0, 6);
    return {
      servidor: srv.nome || '', cpf: srv.cpf || '',
      beneficiario: bf.nome || '',
      municipio: mun.nomeIBGE || '',
      uf: String(uf.sigla || uf.nome || '').slice(0, 2),
      mes: mesRaw, data_saque: reg.dataSaque || '',
      valor: reg.valorSaque ?? reg.valor ?? 0,
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

  // ── Proxy fetch ────────────────────────────────────────────────────
  const proxyFetch = async (endpoint, params, retries = 3) => {
    const res = await fetch('/api/proxy', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint, params, api_key: config.api_key }),
    });
    if (res.status === 429) { await new Promise(r => setTimeout(r, 2000)); return proxyFetch(endpoint, params, retries); }
    if ((res.status === 502 || res.status === 504) && retries > 0) { await new Promise(r => setTimeout(r, 3000)); return proxyFetch(endpoint, params, retries - 1); }
    if (!res.ok) { const t = await res.text(); let msg = t; try { msg = JSON.parse(t)?.detail || t; } catch {} throw new Error(msg); }
    return res.json();
  };

  // ── Crossing logic ─────────────────────────────────────────────────
  const startCrossing = async () => {
    if (!file) return;
    setLoading(true);
    setError(null);
    setStatus({ status: 'processing', progress: 0, result: [], message: 'Iniciando...' });
    setFase('processing');
    setSearchFilter('');
    setAgrupado(false);
    setExpandedRows(new Set());
    cancelRef.current = false;

    try {
      const { rows, sep, headers } = await parseCSV(file);
      const cpfIdx = headers.indexOf(config.col_cpf);
      const nomeIdx = headers.indexOf(config.col_nome);

      const serverMap = new Map();
      rows.slice(1).forEach(row => {
        const cells = row.split(sep);
        const cpf = normalizarCPF(cells[cpfIdx]?.replace(/^"|"$/g, ''));
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
      let lastFlush = 0;

      const flush = (force = false) => {
        if (force || allResults.length - lastFlush >= 5) {
          setStatus(prev => ({ ...prev, result: [...allResults] }));
          lastFlush = allResults.length;
        }
      };

      for (let i = 0; i < meses.length; i++) {
        if (cancelRef.current) break;
        const mes = meses[i];

        if (config.modo === 'municipio') {
          let pagina = 1;
          const MAX_PAGINAS = modoTeste ? 100 : Infinity;
          while (true) {
            if (cancelRef.current) break;
            setStatus(prev => ({
              ...prev,
              progress: Math.round((i / meses.length) * 100),
              message: `${mes} — pág. ${pagina}${modoTeste ? ' · modo teste' : ''}`,
            }));
            const regs = await proxyFetch('municipio', { mesAno: mes, codigoIbge: config.ibge, pagina });
            for (const reg of regs) {
              const bf = reg.beneficiarioNovoBolsaFamilia || {};
              const chave = chaveJS(bf.cpfFormatado || '', bf.nome || '');
              if (chave && serverMap.has(chave))
                for (const srv of serverMap.get(chave)) allResults.push(formatResultJS(srv, reg));
            }
            flush();
            if (regs.length < 15 || pagina >= MAX_PAGINAS) break;
            pagina++;
            await new Promise(r => setTimeout(r, 150));
          }
        } else {
          const todos = [...serverMap.values()].flat();
          const servidores = modoTeste ? todos.slice(0, 500) : todos;
          for (let j = 0; j < servidores.length; j++) {
            if (cancelRef.current) break;
            const srv = servidores[j];
            setStatus(prev => ({
              ...prev,
              progress: Math.round(((i * servidores.length + j) / (meses.length * servidores.length)) * 100),
              message: `${mes} — CPF ${j + 1}/${servidores.length}`,
            }));
            const regs = await proxyFetch('cpf', { cpf: srv.cpf, pagina: 1 });
            for (const reg of regs) {
              const mesRef = (reg.mesReferencia || '').replace(/-/g, '').slice(0, 6);
              if (mesRef !== mes) continue;
              const bf = reg.beneficiarioNovoBolsaFamilia || {};
              if (chaveJS(bf.cpfFormatado || '', bf.nome || '') === chaveJS(srv.cpf, srv.nome))
                allResults.push(formatResultJS(srv, reg));
            }
            flush();
            await new Promise(r => setTimeout(r, 100));
          }
        }
      }

      flush(true);
      setStatus(prev => ({ ...prev, status: cancelRef.current ? 'cancelled' : 'completed', progress: 100 }));
      setFase('done');
      setLoading(false);
    } catch (err) {
      setLoading(false);
      setFase('done');
      setError(err.message);
      setStatus(prev => ({ ...(prev || {}), status: 'failed', result: prev?.result || [] }));
    }
  };

  const handleCancel = () => { cancelRef.current = true; };

  const handleReset = () => {
    cancelRef.current = false;
    setFase('config');
    setStatus(null);
    setError(null);
    setSearchFilter('');
    setAgrupado(false);
    setExpandedRows(new Set());
    setLoading(false);
  };

  // ── Derived data ───────────────────────────────────────────────────
  const allResults = status?.result || [];

  const filteredResults = useMemo(() => {
    if (!searchFilter) return allResults;
    const q = searchFilter.toLowerCase();
    return allResults.filter(r =>
      r.servidor?.toLowerCase().includes(q) ||
      r.cpf?.includes(q) ||
      r.municipio?.toLowerCase().includes(q) ||
      r.beneficiario?.toLowerCase().includes(q)
    );
  }, [allResults, searchFilter]);

  const totalValue = useMemo(() =>
    allResults.reduce((s, r) => s + (r.valor || 0), 0), [allResults]);

  const uniqueServers = useMemo(() =>
    new Set(allResults.map(r => r.cpf)).size, [allResults]);

  const topMes = useMemo(() => {
    const counts = {};
    for (const r of allResults) counts[r.mes] = (counts[r.mes] || 0) + 1;
    const top = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    if (!top?.[0]) return null;
    const m = top[0];
    return `${m.slice(4)}/${m.slice(0, 4)}`;
  }, [allResults]);

  const groupedResults = useMemo(() => {
    const map = new Map();
    for (const r of filteredResults) {
      if (!map.has(r.cpf)) map.set(r.cpf, { servidor: r.servidor, cpf: r.cpf, ocorrencias: [], totalValor: 0 });
      const g = map.get(r.cpf);
      g.ocorrencias.push(r);
      g.totalValor += r.valor || 0;
    }
    return [...map.values()].sort((a, b) => b.totalValor - a.totalValor);
  }, [filteredResults]);

  const toggleRow = (cpf) => setExpandedRows(prev => {
    const next = new Set(prev);
    next.has(cpf) ? next.delete(cpf) : next.add(cpf);
    return next;
  });

  const exportCSV = () => {
    if (!allResults.length) return;
    const hdrs = ['Servidor', 'CPF', 'Beneficiário', 'Município', 'UF', 'Mês Ref.', 'Data Saque', 'Valor'];
    const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const rows = allResults.map(r =>
      [r.servidor, r.cpf, r.beneficiario, r.municipio, r.uf, r.mes, r.data_saque, r.valor].map(esc)
    );
    const csv = '\uFEFF' + [hdrs.join(','), ...rows.map(r => r.join(','))].join('\n');
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8;' }));
    Object.assign(document.createElement('a'), { href: url, download: 'resultados_bolsafamilia.csv' }).click();
    URL.revokeObjectURL(url);
  };

  const fmtMes = (m) => m ? `${m.slice(4)}/${m.slice(0, 4)}` : '—';
  const canStart = file && !loading && config.col_cpf && (config.modo !== 'municipio' || config.ibge);

  // ── Labels for compact banner ──────────────────────────────────────
  const labelPeriodo = `${fmtMes(config.m_ini)} – ${fmtMes(config.m_fim)}`;
  const labelLocal = config.modo === 'municipio'
    ? `${municipioSearch || config.ibge}`
    : 'Por CPF';

  // ══════════════════════════════════════════════════════════════════
  // RENDER: CONFIG
  // ══════════════════════════════════════════════════════════════════
  if (fase === 'config') {
    return (
      <div className="container animate-in">
        <header>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <ShieldCheck size={30} color="#3b82f6" />
            <h1>Portal de Auditoria Bolsa Família</h1>
          </div>
        </header>

        <div className="dashboard-grid">
          <div className="card glass">
            <div className="card-title">Status da API</div>
            {apiHealth === 'checking' && <div className="card-value status-checking"><Loader2 size={18} className="spin" /> Verificando...</div>}
            {apiHealth === 'ok' && <div className="card-value status-ok"><CheckCircle2 size={22} /> Conectado</div>}
            {apiHealth === 'error' && <div className="card-value status-error"><XCircle size={22} /> Offline</div>}
          </div>
          <div className="card glass">
            <div className="card-title">Servidores Carregados</div>
            <div className="card-value">{fileInfo ? fileInfo.total.toLocaleString('pt-BR') : '---'}</div>
            {fileInfo && <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '4px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{fileInfo.filename}</div>}
          </div>
          <div className="card glass">
            <div className="card-title">Alertas Encontrados</div>
            <div className="card-value">0</div>
          </div>
        </div>

        {error && <div className="error-banner"><AlertCircle size={16} /><span>{error}</span></div>}

        <div className="main-grid">
          {/* ── Config sidebar ── */}
          <div className="form-section glass">
            <h2 className="section-title">Configuração</h2>

            <div className="input-group">
              <label>Arquivo de Servidores</label>
              <div className={`upload-zone${file ? ' uploaded' : ''}`}
                onClick={() => document.getElementById('fileInput').click()}
                onDragOver={e => e.preventDefault()} onDrop={handleDrop}>
                <Upload size={22} style={{ color: file ? 'var(--success)' : 'var(--text-dim)' }} />
                <div className="upload-label">{file ? file.name : 'Arraste ou clique para subir CSV / Excel'}</div>
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
              <label>Chave de API (Opcional — lida do servidor se vazia)</label>
              <input type="password" placeholder="Chave do Portal da Transparência"
                value={config.api_key} onChange={e => setConfig({ ...config, api_key: e.target.value })} />
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
              <div className="input-group">
                <label>Mês Início</label>
                <input type="text" placeholder="YYYYMM" value={config.m_ini} maxLength={6}
                  onChange={e => setConfig({ ...config, m_ini: e.target.value })} />
              </div>
              <div className="input-group">
                <label>Mês Fim</label>
                <input type="text" placeholder="YYYYMM" value={config.m_fim} maxLength={6}
                  onChange={e => setConfig({ ...config, m_fim: e.target.value })} />
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
                  <input type="text"
                    placeholder={municipios.length === 0 ? 'Carregando municípios...' : 'Buscar município...'}
                    value={municipioSearch} disabled={municipios.length === 0}
                    onChange={e => {
                      setMunicipioSearch(e.target.value);
                      setShowMunicipioDropdown(true);
                      if (!e.target.value) setConfig(prev => ({ ...prev, ibge: '' }));
                    }}
                    onFocus={() => setShowMunicipioDropdown(true)} />
                  {config.ibge && <span className="municipio-code-badge">{config.ibge}</span>}
                  {showMunicipioDropdown && municipioSearch.trim().length >= 2 && (
                    <ul className="municipio-dropdown">
                      {(() => {
                        const q = municipioSearch.toLowerCase();
                        const filtered = municipios.filter(m =>
                          m.nome.toLowerCase().includes(q) || m.uf.toLowerCase().includes(q) || m.id.includes(municipioSearch)
                        );
                        return filtered.length === 0
                          ? <li className="municipio-empty">Nenhum município encontrado</li>
                          : filtered.slice(0, 50).map(m => (
                            <li key={m.id} className={config.ibge === m.id ? 'selected' : ''}
                              onMouseDown={() => {
                                setConfig(prev => ({ ...prev, ibge: m.id }));
                                setMunicipioSearch(`${m.nome} - ${m.uf}`);
                                setShowMunicipioDropdown(false);
                              }}>
                              <span className="municipio-nome">{m.nome}</span>
                              <span className="municipio-meta">{m.uf} · {m.id}</span>
                            </li>
                          ));
                      })()}
                    </ul>
                  )}
                </div>
              </div>
            )}

            <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontSize: '0.8rem', color: modoTeste ? 'var(--warning)' : 'var(--text-dim)', marginTop: '0.5rem' }}>
              <input type="checkbox" checked={modoTeste} onChange={e => setModoTeste(e.target.checked)} />
              Modo Teste {modoTeste ? '(100 págs/mês · 500 CPFs)' : '— desativado (execução completa)'}
            </label>

            <button className="btn btn-primary" style={{ width: '100%', marginTop: '0.75rem' }}
              disabled={!canStart} onClick={startCrossing}>
              <Search size={15} style={{ marginRight: '8px' }} />Iniciar Cruzamento
            </button>

            {file && !config.col_cpf && (
              <p style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '0.5rem', textAlign: 'center' }}>
                Selecione a coluna de CPF para continuar
              </p>
            )}
            {file && config.col_cpf && config.modo === 'municipio' && !config.ibge && (
              <p style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '0.5rem', textAlign: 'center' }}>
                Selecione um município para continuar
              </p>
            )}
          </div>

          {/* ── Empty results placeholder ── */}
          <div className="glass results-panel">
            <div className="results-header">
              <h2 className="section-title" style={{ marginBottom: 0 }}>Resultados do Cruzamento</h2>
            </div>
            <div className="empty-state">
              <FileText size={44} style={{ opacity: 0.15, marginBottom: '0.75rem' }} />
              <p>Nenhum dado processado ainda.</p>
              <p style={{ fontSize: '0.8rem', marginTop: '0.25rem', color: 'var(--text-dim)' }}>
                Configure os parâmetros e clique em Iniciar.
              </p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // ══════════════════════════════════════════════════════════════════
  // RENDER: PROCESSING / DONE
  // ══════════════════════════════════════════════════════════════════
  const isProcessing = fase === 'processing';

  return (
    <div className="container animate-in">
      <header>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <ShieldCheck size={30} color="#3b82f6" />
          <h1>Portal de Auditoria Bolsa Família</h1>
        </div>
        <button className="btn btn-sm" style={{ gap: '6px', color: 'var(--text-secondary)', border: '1px solid var(--border-color)' }} onClick={handleReset}>
          <RotateCcw size={13} /> Nova Consulta
        </button>
      </header>

      {/* ── Summary cards ── */}
      <div className="dashboard-grid" style={{ gridTemplateColumns: 'repeat(4, 1fr)' }}>
        <div className="card glass">
          <div className="card-title">Servidores Únicos</div>
          <div className="card-value" style={{ color: uniqueServers > 0 ? 'var(--error)' : 'inherit' }}>
            {uniqueServers.toLocaleString('pt-BR')}
          </div>
          <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '4px' }}>com ocorrências no BF</div>
        </div>
        <div className="card glass">
          <div className="card-title">Total de Alertas</div>
          <div className="card-value" style={{ color: allResults.length > 0 ? 'var(--error)' : 'inherit' }}>
            {allResults.length.toLocaleString('pt-BR')}
          </div>
          <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '4px' }}>registros de saque</div>
        </div>
        <div className="card glass">
          <div className="card-title">Mês com Mais Registros</div>
          <div className="card-value" style={{ fontSize: '1.3rem' }}>{topMes || (isProcessing ? '…' : '—')}</div>
          <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '4px' }}>pico de ocorrências</div>
        </div>
        <div className="card glass">
          <div className="card-title">Valor Total em Saques</div>
          <div className="card-value" style={{ fontSize: '1.2rem', color: totalValue > 0 ? 'var(--error)' : 'inherit' }}>
            R$ {totalValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
          </div>
          <div style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '4px' }}>soma do período</div>
        </div>
      </div>

      {/* ── Compact config banner ── */}
      <div className="glass" style={{ padding: '0.9rem 1.5rem', borderRadius: 'var(--radius)', marginBottom: '1.5rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', gap: '1.5rem', flexWrap: 'wrap', fontSize: '0.82rem', color: 'var(--text-secondary)' }}>
          <span><strong style={{ color: 'var(--text-primary)' }}>Arquivo:</strong> {fileInfo?.filename} ({fileInfo?.total.toLocaleString('pt-BR')} serv.)</span>
          <span><strong style={{ color: 'var(--text-primary)' }}>Período:</strong> {labelPeriodo}</span>
          <span><strong style={{ color: 'var(--text-primary)' }}>Local:</strong> {labelLocal}</span>
          {modoTeste && <span style={{ color: 'var(--warning)', fontWeight: 600 }}>⚠ Modo Teste ativo</span>}
        </div>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          {isProcessing && (
            <button className="btn btn-sm" style={{ color: 'var(--error)', border: '1px solid rgba(220,38,38,0.4)', gap: '5px' }} onClick={handleCancel}>
              <X size={13} /> Cancelar
            </button>
          )}
        </div>
      </div>

      {error && <div className="error-banner" style={{ marginBottom: '1.5rem' }}><AlertCircle size={16} /><span>{error}</span></div>}

      {/* ── Results panel ── */}
      <div className="glass results-panel">

        {/* Progress */}
        {isProcessing && (
          <div style={{ marginBottom: allResults.length > 0 ? '1.5rem' : '0' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.4rem' }}>
              <span style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', display: 'flex', alignItems: 'center', gap: '6px' }}>
                <Loader2 size={13} className="spin" />
                {status?.message || 'Consultando API...'}
              </span>
              <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text-secondary)' }}>{status?.progress || 0}%</span>
            </div>
            <div className="progress-bar-bg" style={{ margin: 0 }}>
              <div className="progress-bar-fill" style={{ width: `${status?.progress || 0}%` }} />
            </div>
            {allResults.length > 0 && (
              <p style={{ fontSize: '0.72rem', color: 'var(--text-dim)', marginTop: '0.4rem' }}>
                {allResults.length} correspondência{allResults.length !== 1 ? 's' : ''} encontrada{allResults.length !== 1 ? 's' : ''} até agora...
              </p>
            )}
          </div>
        )}

        {/* Cancelled notice */}
        {!isProcessing && status?.status === 'cancelled' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '0.75rem 1rem', background: 'rgba(217,119,6,0.1)', border: '1px solid rgba(217,119,6,0.3)', borderRadius: '8px', marginBottom: '1.25rem', fontSize: '0.85rem', color: 'var(--warning)' }}>
            <AlertCircle size={15} /> Consulta cancelada. Exibindo resultados parciais.
          </div>
        )}

        {/* Completed, no results */}
        {!isProcessing && status?.status === 'completed' && allResults.length === 0 && (
          <div className="empty-state" style={{ color: 'var(--success)' }}>
            <CheckCircle2 size={44} style={{ opacity: 0.5, marginBottom: '0.75rem' }} />
            <p style={{ fontWeight: 600 }}>Nenhum servidor encontrado como beneficiário.</p>
            <p style={{ fontSize: '0.8rem', marginTop: '0.25rem' }}>Cruzamento concluído sem alertas.</p>
          </div>
        )}

        {/* Results */}
        {allResults.length > 0 && (
          <>
            <div className="results-header">
              <h2 className="section-title" style={{ marginBottom: 0 }}>
                {isProcessing ? 'Resultados parciais' : 'Resultados do Cruzamento'}
              </h2>
              <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                <button className="btn btn-sm"
                  style={{ border: '1px solid var(--border-color)', gap: '6px', background: agrupado ? 'rgba(37,99,235,0.1)' : 'transparent', color: agrupado ? '#2563eb' : 'var(--text-secondary)' }}
                  onClick={() => { setAgrupado(a => !a); setExpandedRows(new Set()); }}>
                  <Users size={13} /> {agrupado ? 'Ver todos' : 'Agrupar por servidor'}
                </button>
                <button className="btn btn-primary btn-sm" onClick={exportCSV} style={{ gap: '5px' }}>
                  <Download size={13} /> Exportar CSV
                </button>
              </div>
            </div>

            <div style={{ position: 'relative', marginBottom: '1rem' }}>
              <Filter size={13} className="filter-icon" />
              <input type="text" placeholder="Filtrar por nome, CPF ou município..."
                value={searchFilter} onChange={e => setSearchFilter(e.target.value)}
                style={{ paddingLeft: '34px' }} />
            </div>

            {/* Flat table */}
            {!agrupado && (
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
                        <td>{fmtMes(row.mes)}</td>
                        <td>{row.data_saque || '—'}</td>
                        <td className="valor-cell">R$ {(row.valor || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Grouped table */}
            {agrupado && (
              <div className="results-container">
                <table>
                  <thead>
                    <tr>
                      <th style={{ width: '28px' }}></th>
                      <th>Servidor</th>
                      <th>CPF</th>
                      <th>Ocorrências</th>
                      <th>Meses</th>
                      <th>Valor Total</th>
                    </tr>
                  </thead>
                  <tbody>
                    {groupedResults.map(g => {
                      const expanded = expandedRows.has(g.cpf);
                      const meses = [...new Set(g.ocorrencias.map(o => o.mes))].sort().map(fmtMes);
                      return (
                        <React.Fragment key={g.cpf}>
                          <tr onClick={() => toggleRow(g.cpf)} style={{ cursor: 'pointer', background: expanded ? 'rgba(37,99,235,0.04)' : undefined }}>
                            <td style={{ textAlign: 'center', color: 'var(--text-dim)' }}>
                              {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                            </td>
                            <td style={{ fontWeight: 600 }}>{g.servidor}</td>
                            <td style={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{g.cpf}</td>
                            <td>
                              <span style={{ display: 'inline-block', padding: '2px 10px', borderRadius: '20px', background: 'rgba(220,38,38,0.12)', color: 'var(--error)', fontSize: '0.75rem', fontWeight: 700 }}>
                                {g.ocorrencias.length}×
                              </span>
                            </td>
                            <td style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>{meses.join(', ')}</td>
                            <td className="valor-cell">R$ {g.totalValor.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                          </tr>
                          {expanded && g.ocorrencias.map((o, i) => (
                            <tr key={i} style={{ background: 'rgba(37,99,235,0.025)' }}>
                              <td></td>
                              <td colSpan={2} style={{ paddingLeft: '2rem', fontSize: '0.78rem', color: 'var(--text-secondary)' }}>
                                {o.beneficiario || '—'}
                              </td>
                              <td style={{ fontSize: '0.78rem', color: 'var(--text-dim)' }}>{o.municipio}{o.uf ? ` / ${o.uf}` : ''}</td>
                              <td style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>{fmtMes(o.mes)} · {o.data_saque || '—'}</td>
                              <td className="valor-cell" style={{ fontSize: '0.78rem' }}>R$ {(o.valor || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                            </tr>
                          ))}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}

            {searchFilter && (
              <p style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '0.5rem', textAlign: 'right' }}>
                {agrupado
                  ? `${groupedResults.length} de ${uniqueServers} servidores`
                  : `${filteredResults.length} de ${allResults.length} registros`}
              </p>
            )}

            <div className="summary-bar">
              <span>
                {uniqueServers} servidor{uniqueServers !== 1 ? 'es' : ''} único{uniqueServers !== 1 ? 's' : ''} · {allResults.length} ocorrência{allResults.length !== 1 ? 's' : ''}
              </span>
              <span>Valor total: <strong>R$ {totalValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong></span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
