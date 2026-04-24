import React, { useState, useEffect, useRef, useMemo } from 'react';
import {
  Upload, Search, Download, AlertCircle, CheckCircle2,
  ShieldCheck, Loader2, Filter, FileText,
  ChevronDown, ChevronRight, RotateCcw, Users, X, Settings,
  Database,
} from 'lucide-react';

// Número de meses buscados em paralelo (par/ímpar simultâneos)
const PARALLEL_WORKERS = 3;

export default function App() {
  // ── Fonte dos servidores ─────────────────────
  const [fonteServidores, setFonteServidores] = useState('oracle'); // 'oracle' | 'csv'
  const [oracleConfig, setOracleConfig] = useState({ ent_codigo: '1118181', exercicio: '2024' });
  const [oracleInfo, setOracleInfo] = useState(null); // { total } depois de carregar

  // ── Arquivo CSV (fonte alternativa) ──────────
  const [file, setFile] = useState(null);
  const [columns, setColumns] = useState([]);
  const [fileInfo, setFileInfo] = useState(null);

  // ── Estado geral ─────────────────────────────
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState(null);
  const [apiHealth, setApiHealth] = useState('checking');
  const [error, setError] = useState(null);
  const [searchFilter, setSearchFilter] = useState('');

  // ── Seletor de município ─────────────────────
  const [municipios, setMunicipios] = useState([]);
  const [municipioSearch, setMunicipioSearch] = useState('');
  const [showMunicipioDropdown, setShowMunicipioDropdown] = useState(false);
  const municipioRef = useRef(null);

  // ── Config consulta ──────────────────────────
  const [modoTeste, setModoTeste] = useState(true);
  const [showBancoConfig, setShowBancoConfig] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [config, setConfig] = useState({
    m_ini: '202401', m_fim: '202403',
    modo: 'municipio', ibge: '', api_key: '', col_cpf: '', col_nome: '',
  });

  // ── Resultados ───────────────────────────────
  const [fase, setFase] = useState('config');
  const [agrupado, setAgrupado] = useState(false);
  const [expandedRows, setExpandedRows] = useState(new Set());
  const [apenasMultiplos, setApenasMultiplos] = useState(false);
  const cancelRef = useRef(false);

  // ── Health checks ────────────────────────────
  useEffect(() => {
    fetch('/api/health')
      .then(r => r.ok ? setApiHealth('ok') : setApiHealth('error'))
      .catch(() => setApiHealth('error'));
  }, []);

  useEffect(() => {
    fetch('https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome')
      .then(r => r.json())
      .then(data => setMunicipios(data.map(m => ({
        id: String(m.id), nome: m.nome,
        uf: m.microrregiao?.mesorregiao?.UF?.sigla || '',
      }))))
      .catch(() => {});
  }, []);

  useEffect(() => {
    const h = e => { if (municipioRef.current && !municipioRef.current.contains(e.target)) setShowMunicipioDropdown(false); };
    document.addEventListener('mousedown', h);
    return () => document.removeEventListener('mousedown', h);
  }, []);

  // ── Utilitários ──────────────────────────────
  const normalizarCPF = cpf => { if (!cpf) return ''; const d = String(cpf).replace(/\D/g, ''); return d.length <= 11 ? d.padStart(11, '0').slice(0, 11) : d.slice(0, 11); };
  const meioCPF = cpfRaw => { const raw = String(cpfRaw || ''); const full = raw.replace(/\D/g, ''); if (full.length === 11) return full.slice(3, 9); return raw.replace(/[xX*]/g, '').replace(/\D/g, '').slice(0, 6); };
  const primeiroNome = nome => nome ? String(nome).trim().split(/\s+/)[0].toUpperCase() : '';
  const chaveJS = (cpfRaw, nome) => { const meio = meioCPF(cpfRaw), pnome = primeiroNome(nome); return meio && pnome ? `${meio}|${pnome}` : ''; };
  const fmtMes = m => m ? `${m.slice(4)}/${m.slice(0, 4)}` : '—';
  const delay = ms => new Promise(r => setTimeout(r, ms));

  const getMesesList = (ini, fim) => {
    const meses = []; let [y, m] = [parseInt(ini.slice(0, 4)), parseInt(ini.slice(4))];
    const [ey, em] = [parseInt(fim.slice(0, 4)), parseInt(fim.slice(4))];
    while (y < ey || (y === ey && m <= em)) { meses.push(`${y}${String(m).padStart(2, '0')}`); m++; if (m > 12) { m = 1; y++; } }
    return meses;
  };

  // ── Parsing CSV ──────────────────────────────
  const parseCSV = f => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = e => {
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

  const handleFileUpload = async e => {
    const f = e.target.files[0];
    if (!f) return;
    if (f.size > 10 * 1024 * 1024) { setError(`Arquivo muito grande (${(f.size / 1024 / 1024).toFixed(1)} MB). Limite: 10 MB.`); return; }
    setFile(f); setColumns([]); setFileInfo(null); setError(null); setStatus(null);
    const suffix = f.name.split('.').pop().toLowerCase();
    if (suffix === 'csv') {
      try {
        const { headers, total } = await parseCSV(f);
        setColumns(headers);
        setFileInfo({ total, filename: f.name });
        setConfig(prev => ({
          ...prev,
          col_cpf: headers.find(c => /cpf/i.test(c)) || '',
          col_nome: headers.find(c => /nome/i.test(c)) || '',
        }));
      } catch (err) { setError('Erro ao ler CSV: ' + err.message); }
    } else {
      const fd = new FormData(); fd.append('file', f);
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: fd });
        if (!res.ok) throw new Error('Erro ao ler arquivo');
        const data = await res.json();
        setColumns(data.columns);
        setFileInfo({ total: data.total, filename: data.filename });
        setConfig(prev => ({
          ...prev,
          col_cpf: data.columns.find(c => /cpf/i.test(c)) || '',
          col_nome: data.columns.find(c => /nome/i.test(c)) || '',
        }));
      } catch (err) { setError(err.message); }
    }
  };

  const handleDrop = e => { e.preventDefault(); const f = e.dataTransfer.files[0]; if (f) handleFileUpload({ target: { files: [f] } }); };

  // ── Formato resultado ────────────────────────
  const formatResultJS = (srv, reg, pagina = null) => {
    // O Portal da Transparência às vezes inverte sigla (Mato Grosso) e nome (MT)
    const ufAbbr = String(uf.nome || '').length === 2 ? uf.nome : (String(uf.sigla || '').length === 2 ? uf.sigla : String(uf.sigla || '').slice(0, 2));
    
    return {
      servidor: srv.nome || '', cpf: srv.cpf || '', beneficiario: bf.nome || '',
      nis: bf.nis || bf.ns || bf.numeroInscricaoSocial || '',
      municipio: mun.nomeIBGE || '', uf: ufAbbr,
      mes: (reg.dataMesReferencia || reg.mesReferencia || '').replace(/-/g, '').slice(0, 6),
      data_saque: reg.dataSaque || '', valor: reg.valorSaque ?? reg.valor ?? 0,
      pagina,
    };
  };

  // ── Proxy API Portal da Transparência ────────
  const proxyFetch = async (endpoint, params, retries = 3) => {
    const res = await fetch('/api/proxy', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint, params, api_key: config.api_key }),
    });
    if (res.status === 429) { await delay(2000); return proxyFetch(endpoint, params, retries); }
    if ((res.status === 502 || res.status === 504) && retries > 0) { await delay(3000); return proxyFetch(endpoint, params, retries - 1); }
    if (!res.ok) { const t = await res.text(); let msg = t; try { msg = JSON.parse(t)?.detail || t; } catch {} throw new Error(msg); }
    return res.json();
  };

  // ── Constrói serverMap (Oracle ou CSV) ───────
  const buildServerMap = async () => {
    const map = new Map();
    const add = (cpfRaw, nome) => {
      const cpf = normalizarCPF(cpfRaw);
      if (!cpf) return;
      const chave = chaveJS(cpf, nome);
      if (chave) { if (!map.has(chave)) map.set(chave, []); map.get(chave).push({ cpf, nome }); }
    };

    if (fonteServidores === 'oracle') {
      setStatus(prev => ({ ...prev, message: 'Carregando servidores do Oracle...' }));
      const res = await fetch('/api/servidores', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(oracleConfig),
      });
      if (!res.ok) { const t = await res.text(); throw new Error(JSON.parse(t)?.detail || t); }
      const { servidores, total } = await res.json();
      setOracleInfo({ total });
      servidores.forEach(s => add(s.cpf, s.nome));
      return { serverMap: map, totalServidores: total };
    } else {
      const { rows, sep, headers } = await parseCSV(file);
      const cpfIdx = headers.indexOf(config.col_cpf);
      const nomeIdx = headers.indexOf(config.col_nome);
      rows.slice(1).forEach(row => {
        const cells = row.split(sep);
        add(cells[cpfIdx]?.replace(/^"|"$/g, ''), nomeIdx !== -1 ? (cells[nomeIdx]?.replace(/^"|"$/g, '') || '') : '');
      });
      return { serverMap: map, totalServidores: rows.length - 1 };
    }
  };

  // ── Cruzamento principal ──────────────────────
  const startCrossing = async () => {
    if (fonteServidores === 'csv' && !file) return;
    setLoading(true); setError(null);
    setStatus({ status: 'processing', progress: 0, result: [], message: 'Iniciando...' });
    setFase('processing'); setSearchFilter(''); setAgrupado(false); setExpandedRows(new Set());
    cancelRef.current = false;

    try {
      // 1. Carrega servidores (Oracle ou CSV)
      const { serverMap, totalServidores } = await buildServerMap();
      if (!serverMap.size) throw new Error('Nenhum servidor carregado — verifique a fonte de dados.');

      const meses = getMesesList(config.m_ini, config.m_fim);
      const allResults = [];
      const seenResults = new Set();
      let mesesConcluidos = 0;

      const flush = () => setStatus(prev => ({ ...prev, result: [...allResults] }));

      // 2. Busca paralela por município ─────────
      if (config.modo === 'municipio') {
        const MAX_PAGINAS = modoTeste ? 100 : Infinity;

        // Busca todas as páginas de UM mês
        const fetchMes = async mes => {
          if (cancelRef.current) return;
          let pagina = 1;
          while (true) {
            if (cancelRef.current) break;
            const regs = await proxyFetch('municipio', { mesAno: mes, codigoIbge: config.ibge, pagina });
            for (const reg of regs) {
              const bf = reg.beneficiarioNovoBolsaFamilia || {};
              const chave = chaveJS(bf.cpfFormatado || '', bf.nome || '');
              if (chave && serverMap.has(chave)) {
                for (const srv of serverMap.get(chave)) {
                  const deduKey = `${srv.cpf}|${mes}|${reg.dataSaque || ''}|${reg.valorSaque ?? reg.valor ?? 0}`;
                  if (seenResults.has(deduKey)) continue;
                  seenResults.add(deduKey);
                  allResults.push(formatResultJS(srv, reg, pagina));
                }
              }
            }
            flush();
            if (regs.length < 15 || pagina >= MAX_PAGINAS) break;
            pagina++;
            await delay(150);
          }
          mesesConcluidos++;
          setStatus(prev => ({
            ...prev,
            progress: Math.round((mesesConcluidos / meses.length) * 100),
            message: `${mesesConcluidos}/${meses.length} meses concluídos`,
          }));
        };

        // Processa em lotes de PARALLEL_WORKERS (par e ímpar simultâneos)
        for (let i = 0; i < meses.length; i += PARALLEL_WORKERS) {
          if (cancelRef.current) break;
          const lote = meses.slice(i, i + PARALLEL_WORKERS);
          setStatus(prev => ({
            ...prev,
            message: `Buscando em paralelo: ${lote.map(fmtMes).join(' · ')}`,
          }));
          await Promise.all(lote.map(mes => fetchMes(mes)));
        }

      // 3. Busca sequencial por CPF ─────────────
      } else {
        const todos = [...serverMap.values()].flat();
        const servidores = modoTeste ? todos.slice(0, 500) : todos;
        for (let i = 0; i < meses.length; i++) {
          const mes = meses[i];
          for (let j = 0; j < servidores.length; j++) {
            if (cancelRef.current) break;
            const srv = servidores[j];
            setStatus(prev => ({
              ...prev,
              progress: Math.round(((i * servidores.length + j) / (meses.length * servidores.length)) * 100),
              message: `${fmtMes(mes)} — CPF ${j + 1}/${servidores.length}`,
            }));
            const regs = await proxyFetch('cpf', { cpf: srv.cpf, pagina: 1 });
            for (const reg of regs) {
              const mesRef = (reg.mesReferencia || '').replace(/-/g, '').slice(0, 6);
              if (mesRef !== mes) continue;
              const bf = reg.beneficiarioNovoBolsaFamilia || {};
              if (chaveJS(bf.cpfFormatado || '', bf.nome || '') === chaveJS(srv.cpf, srv.nome)) {
                const deduKey = `${srv.cpf}|${mesRef}|${reg.dataSaque || ''}|${reg.valorSaque ?? reg.valor ?? 0}`;
                if (seenResults.has(deduKey)) continue;
                seenResults.add(deduKey);
                allResults.push(formatResultJS(srv, reg));
              }
            }
            flush(); await delay(100);
          }
        }
      }

      flush();
      setStatus(prev => ({ ...prev, status: cancelRef.current ? 'cancelled' : 'completed', progress: 100 }));
      setFase('done'); setLoading(false);
    } catch (err) {
      setLoading(false); setFase('done'); setError(err.message);
      setStatus(prev => ({ ...(prev || {}), status: 'failed', result: prev?.result || [] }));
    }
  };

  const handleCancel = () => { cancelRef.current = true; };
  const handleReset = () => {
    cancelRef.current = false;
    setFase('config'); setStatus(null); setError(null); setSearchFilter('');
    setAgrupado(false); setExpandedRows(new Set()); setApenasMultiplos(false); setLoading(false);
    setOracleInfo(null);
  };

  // ── Dados derivados ──────────────────────────
  const allResults = status?.result || [];
  const filteredResults = useMemo(() => {
    if (!searchFilter) return allResults;
    const q = searchFilter.toLowerCase();
    return allResults.filter(r => r.servidor?.toLowerCase().includes(q) || r.cpf?.includes(q) || r.municipio?.toLowerCase().includes(q) || r.beneficiario?.toLowerCase().includes(q));
  }, [allResults, searchFilter]);

  const totalValue   = useMemo(() => allResults.reduce((s, r) => s + (r.valor || 0), 0), [allResults]);
  const uniqueServers = useMemo(() => new Set(allResults.map(r => r.cpf)).size, [allResults]);
  const topMes = useMemo(() => {
    const counts = {}; for (const r of allResults) counts[r.mes] = (counts[r.mes] || 0) + 1;
    const top = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    return top?.[0] ? fmtMes(top[0]) : null;
  }, [allResults]);

  const groupedResults = useMemo(() => {
    const map = new Map();
    for (const r of filteredResults) {
      // Agrupamos pela chave (Nome + CPF) para garantir que é o mesmo servidor
      const key = `${r.servidor}|${r.cpf}`;
      if (!map.has(key)) {
        map.set(key, { 
          servidor: r.servidor, 
          cpf: r.cpf, 
          nis: r.nis || '', 
          nisSet: new Set(), 
          ocorrencias: [], 
          totalValor: 0 
        });
      }
      const g = map.get(key);
      if (!g.nis && r.nis) g.nis = r.nis;
      if (r.nis) g.nisSet.add(r.nis);
      g.ocorrencias.push(r);
      g.totalValor += r.valor || 0;
    }
    const all = [...map.values()]
      .map(g => ({ ...g, nisCount: g.nisSet.size }))
      .sort((a, b) => b.ocorrencias.length - a.ocorrencias.length || b.totalValor - a.totalValor);
    return apenasMultiplos ? all.filter(g => g.ocorrencias.length > 5) : all;
  }, [filteredResults, apenasMultiplos]);

  const toggleRow = cpf => setExpandedRows(prev => { const next = new Set(prev); next.has(cpf) ? next.delete(cpf) : next.add(cpf); return next; });

  const exportCSV = () => {
    if (!allResults.length) return;
    const hdrs = ['Servidor', 'CPF', 'NIS', 'Beneficiário', 'Município', 'UF', 'Mês Ref.', 'Data Saque', 'Valor', 'Página'];
    const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const csv = '﻿' + [hdrs.join(','), ...allResults.map(r => [r.servidor, r.cpf, r.nis, r.beneficiario, r.municipio, r.uf, r.mes, r.data_saque, r.valor, r.pagina ?? ''].map(esc).join(','))].join('\n');
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8;' }));
    Object.assign(document.createElement('a'), { href: url, download: 'resultados_bolsafamilia.csv' }).click();
    URL.revokeObjectURL(url);
  };

  // canStart: Oracle precisa só do ibge; CSV precisa do arquivo + col_cpf
  const canStart = !loading && (config.modo !== 'municipio' || config.ibge) && (
    fonteServidores === 'oracle'
      ? true
      : (file && config.col_cpf)
  );

  const labelPeriodo = `${fmtMes(config.m_ini)} – ${fmtMes(config.m_fim)}`;
  const labelLocal   = config.modo === 'municipio' ? (municipioSearch || config.ibge || '—') : 'Por CPF';
  const labelFonte   = fonteServidores === 'oracle'
    ? `Oracle · entidade ${oracleConfig.ent_codigo} / ${oracleConfig.exercicio}`
    : (fileInfo?.filename || 'CSV');
  const isProcessing = fase === 'processing';

  // ═══════════════════════════════════════════════════════
  // TOPBAR
  // ═══════════════════════════════════════════════════════
  const Topbar = () => (
    <div className="topbar">
      <div className="topbar-brand">
        <ShieldCheck size={18} color="#60a5fa" />
        Auditoria Bolsa Família
        <span>|</span>
        <span style={{ opacity: 0.6, fontSize: '0.78rem' }}>Portal da Transparência</span>
      </div>
      <div className="topbar-right">
        {fase !== 'config' && (
          <button className="topbar-btn" onClick={handleReset}>
            <RotateCcw size={12} /> Nova Consulta
          </button>
        )}
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '0.75rem', opacity: 0.7 }}>
          {apiHealth === 'ok'       && <><div className="dot green" /> API conectada</>}
          {apiHealth === 'error'    && <><div className="dot red"   /> API offline</>}
          {apiHealth === 'checking' && <><div className="dot amber" /> Verificando</>}
        </div>
      </div>
    </div>
  );

  // ═══════════════════════════════════════════════════════
  // CONFIG PHASE
  // ═══════════════════════════════════════════════════════
  if (fase === 'config') return (
    <>
      <Topbar />
      <div className="page fade-up">
        {error && <div className="alert alert-red" style={{ marginTop: '1.5rem' }}><AlertCircle size={15} style={{ flexShrink: 0, marginTop: 1 }} /><span>{error}</span></div>}

        <div className="layout" style={{ marginTop: error ? '1rem' : '1.75rem' }}>
          <div className="config-panel">
            <div className="config-panel-header">
              <span>Configurar consulta</span>
            </div>
            <div className="config-panel-body">

              {/* ── Fonte dos servidores ── */}
              <div className="field">
                <label>Fonte dos servidores</label>
                <div className="source-toggle" style={{ marginTop: '0.25rem' }}>
                  <button
                    className={`source-btn ${fonteServidores === 'oracle' ? 'active' : ''}`}
                    onClick={() => setFonteServidores('oracle')}>
                    <Database size={13} /> Oracle
                  </button>
                  <button
                    className={`source-btn ${fonteServidores === 'csv' ? 'active' : ''}`}
                    onClick={() => setFonteServidores('csv')}>
                    <Upload size={13} /> CSV / Excel
                  </button>
                </div>
              </div>

              {/* ── Configurações do Banco (Oracle) ── */}
              {fonteServidores === 'oracle' && (
                <div style={{ marginBottom: '1.25rem' }}>
                  <button type="button" className="advanced-toggle" onClick={() => setShowBancoConfig(a => !a)}>
                    <ChevronRight size={11} style={{ transform: showBancoConfig ? 'rotate(90deg)' : 'none', transition: 'transform 0.2s' }} />
                    <Database size={11} /> Configurações do Banco
                  </button>
                  {showBancoConfig && (
                    <div className="mapping-box fade-in" style={{ marginTop: '0.5rem' }}>
                      <div className="field" style={{ marginBottom: '0.6rem' }}>
                        <label>Código da Entidade</label>
                        <input type="text" value={oracleConfig.ent_codigo}
                          onChange={e => setOracleConfig(p => ({ ...p, ent_codigo: e.target.value }))} />
                      </div>
                      <div className="field" style={{ marginBottom: 0 }}>
                        <label>Exercício (ano)</label>
                        <input type="text" maxLength={4} value={oracleConfig.exercicio}
                          onChange={e => setOracleConfig(p => ({ ...p, exercicio: e.target.value }))} />
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* ── CSV config ── */}
              {fonteServidores === 'csv' && (
                <>
                  <div className="field">
                    <label>Arquivo de servidores</label>
                    <div className={`upload-zone${file ? ' has-file' : ''}`}
                      onClick={() => document.getElementById('fileInput').click()}
                      onDragOver={e => e.preventDefault()} onDrop={handleDrop}>
                      <Upload size={20} style={{ color: file ? 'var(--green)' : 'var(--text-3)' }} />
                      <div className="upload-name">{file ? file.name : 'Arraste ou clique — CSV ou Excel'}</div>
                      {fileInfo && <span className="badge badge-green">{fileInfo.total.toLocaleString('pt-BR')} registros</span>}
                      <input id="fileInput" type="file" hidden onChange={handleFileUpload} accept=".csv,.xlsx,.xls" />
                    </div>
                  </div>
                  {columns.length > 0 && (
                    <div className="mapping-box">
                      <div className="mapping-title">Mapeamento de colunas</div>
                      <div className="field" style={{ marginBottom: '0.7rem' }}>
                        <label>Coluna de CPF *</label>
                        <select value={config.col_cpf} onChange={e => setConfig({ ...config, col_cpf: e.target.value })}>
                          <option value="">— Selecione —</option>
                          {columns.map(c => <option key={c} value={c}>{c}</option>)}
                        </select>
                      </div>
                      <div className="field" style={{ marginBottom: 0 }}>
                        <label>Coluna de Nome</label>
                        <select value={config.col_nome} onChange={e => setConfig({ ...config, col_nome: e.target.value })}>
                          <option value="">— Selecione —</option>
                          {columns.map(c => <option key={c} value={c}>{c}</option>)}
                        </select>
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* ── Período e Modo ── */}
              <div className="field">
                <label>Mês de Referência (YYYYMM)</label>
                <div className="field-row">
                  <input type="text" placeholder="Início" value={config.m_ini} maxLength={6} onChange={e => setConfig({ ...config, m_ini: e.target.value })} />
                  <input type="text" placeholder="Fim" value={config.m_fim} maxLength={6} onChange={e => setConfig({ ...config, m_fim: e.target.value })} />
                </div>
              </div>

              <div className="field">
                <label>Modo de cruzamento</label>
                <div className="mode-toggle">
                  <button className={config.modo === 'municipio' ? 'active' : ''} onClick={() => setConfig({...config, modo: 'municipio'})}>Município</button>
                  <button className={config.modo === 'cpf' ? 'active' : ''} onClick={() => setConfig({...config, modo: 'cpf'})}>Individual (CPF)</button>
                </div>
              </div>

              {/* ── Seletor município ── */}
              {config.modo === 'municipio' && (
                <div className="field" ref={municipioRef}>
                  <label>
                    Município
                    <span className="label-tag">Mato Grosso · MT</span>
                  </label>
                  <div className="combobox full-width">
                    <Search className="search-icon-inside" size={16} />
                    <input type="text"
                      className="input-large"
                      placeholder={municipios.length === 0 ? 'Carregando municípios...' : 'Digite o nome do município...'}
                      value={municipioSearch} disabled={municipios.length === 0}
                      onChange={e => { setMunicipioSearch(e.target.value); setShowMunicipioDropdown(true); if (!e.target.value) setConfig(p => ({ ...p, ibge: '' })); }}
                      onFocus={() => setShowMunicipioDropdown(true)} />
                    {config.ibge && <span className="combobox-badge">{config.ibge}</span>}
                    {showMunicipioDropdown && municipioSearch.trim().length >= 2 && (() => {
                      const q = municipioSearch.toLowerCase();
                      const filtered = municipios.filter(m =>
                        m.uf === 'MT' &&
                        (m.nome.toLowerCase().includes(q) || m.id.includes(municipioSearch))
                      );
                      return (
                        <ul className="combobox-dropdown dropdown-large">
                          {filtered.length === 0
                            ? <li className="city-empty">Nenhum município encontrado em MT</li>
                            : filtered.slice(0, 50).map(m => (
                              <li key={m.id} className={config.ibge === m.id ? 'active' : ''}
                                onMouseDown={() => { setConfig(p => ({ ...p, ibge: m.id })); setMunicipioSearch(m.nome); setShowMunicipioDropdown(false); }}>
                                <span className="city-name" style={{ fontWeight: 600 }}>{m.nome}</span>
                                <span className="city-meta">{m.id}</span>
                              </li>
                            ))}
                        </ul>
                      );
                    })()}
                  </div>
                </div>
              )}

              <div className="divider" />

              {/* ── Avançado ── */}
              <button type="button" className="advanced-toggle" onClick={() => setShowAdvanced(a => !a)}>
                <ChevronRight size={11} style={{ transform: showAdvanced ? 'rotate(90deg)' : 'none', transition: 'transform 0.2s' }} />
                <Settings size={11} /> Configurações avançadas
              </button>
              {showAdvanced && (
                <div className="field" style={{ marginBottom: '0.75rem' }}>
                  <label>Chave de API (sobrepõe a do servidor)</label>
                  <input type="password" placeholder="Chave do Portal da Transparência" value={config.api_key} onChange={e => setConfig({ ...config, api_key: e.target.value })} />
                </div>
              )}

              <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontSize: '0.75rem', color: modoTeste ? 'var(--amber)' : 'var(--text-3)', marginBottom: '1rem', fontWeight: 500 }}>
                <input type="checkbox" checked={modoTeste} onChange={e => setModoTeste(e.target.checked)} />
                {modoTeste ? '⚠ Modo Teste — 100 págs/mês · 500 CPFs' : 'Modo Teste desativado — execução completa'}
              </label>

              <button className="btn btn-primary btn-full" disabled={!canStart} onClick={startCrossing}>
                {loading ? <Loader2 size={14} className="spin" /> : <Search size={14} />}
                Iniciar Cruzamento
              </button>

              {fonteServidores === 'csv' && file && !config.col_cpf &&
                <p style={{ fontSize: '0.72rem', color: 'var(--text-3)', marginTop: '0.5rem', textAlign: 'center' }}>Selecione a coluna de CPF para continuar</p>}
              {config.modo === 'municipio' && !config.ibge &&
                <p style={{ fontSize: '0.72rem', color: 'var(--text-3)', marginTop: '0.5rem', textAlign: 'center' }}>Selecione um município para continuar</p>}
            </div>
          </div>

          {/* Painel de resultados vazio */}
          <div className="results-panel">
            <div className="results-panel-header">
              <span className="results-panel-title">Resultados</span>
            </div>
            <div className="empty">
              <FileText size={40} style={{ opacity: 0.12, marginBottom: '0.25rem' }} />
              <p>Nenhum dado processado ainda.</p>
              <p className="sub">Configure os parâmetros ao lado e clique em Iniciar.</p>
            </div>
          </div>
        </div>
      </div>
    </>
  );

  // ═══════════════════════════════════════════════════════
  // PROCESSING / DONE PHASE
  // ═══════════════════════════════════════════════════════
  return (
    <>
      <Topbar />
      <div className="page fade-up">
        <div className="stats-row cols-4" style={{ marginTop: '1.5rem' }}>
          <div className="stat-card">
            <div className="stat-label">Servidores únicos</div>
            <div className={`stat-value${uniqueServers > 0 ? ' danger' : ''}`}>{uniqueServers.toLocaleString('pt-BR')}</div>
            <div className="stat-sub">com ocorrências no BF</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Total de alertas</div>
            <div className={`stat-value${allResults.length > 0 ? ' danger' : ''}`}>{allResults.length.toLocaleString('pt-BR')}</div>
            <div className="stat-sub">registros de saque</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Mês com mais registros</div>
            <div className="stat-value muted">{topMes || (isProcessing ? '…' : '—')}</div>
            <div className="stat-sub">pico de ocorrências</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Valor total em saques</div>
            <div className={`stat-value muted${totalValue > 0 ? ' danger' : ''}`} style={{ fontSize: '1.25rem' }}>
              {totalValue > 0 ? `R$ ${totalValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}` : '—'}
            </div>
            <div className="stat-sub">soma do período</div>
          </div>
        </div>

        <div className="config-banner">
          <div className="config-banner-info">
            <span><strong>Servidores:</strong> {labelFonte}{oracleInfo ? ` (${oracleInfo.total.toLocaleString('pt-BR')} carregados)` : ''}</span>
            <span><strong>Período:</strong> {labelPeriodo}</span>
            <span><strong>Local:</strong> {labelLocal}</span>
            {modoTeste && <span className="badge badge-amber">Modo Teste</span>}
          </div>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            {isProcessing && <button className="btn btn-sm btn-danger-ghost" onClick={handleCancel}><X size={12} /> Cancelar</button>}
          </div>
        </div>

        {error && <div className="alert alert-red"><AlertCircle size={15} style={{ flexShrink: 0 }} /><span>{error}</span></div>}

        <div className="results-panel">
          <div className="results-panel-header">
            <span className="results-panel-title">{isProcessing ? 'Processando…' : 'Resultados do cruzamento'}</span>
            {allResults.length > 0 && (
              <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                <button className={`btn btn-sm ${agrupado ? 'btn-active' : 'btn-ghost'}`}
                  onClick={() => { setAgrupado(a => !a); setExpandedRows(new Set()); }}>
                  <Users size={12} /> {agrupado ? 'Ver todos' : 'Agrupar por servidor'}
                </button>
                {agrupado && (
                  <button className={`btn btn-sm ${apenasMultiplos ? 'btn-danger-ghost btn-active' : 'btn-ghost'}`}
                    onClick={() => setApenasMultiplos(a => !a)}
                    title="Exibir apenas servidores com mais de 5 ocorrências">
                    ⚠ {apenasMultiplos ? 'Todos' : '> 5 ocorrências'}
                  </button>
                )}
                <button className="btn btn-sm btn-primary" onClick={exportCSV}>
                  <Download size={12} /> Exportar CSV
                </button>
              </div>
            )}
          </div>
          <div className="results-panel-body">

            {isProcessing && (
              <div className="progress-wrap">
                <div className="progress-meta">
                  <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                    <Loader2 size={12} className="spin" />{status?.message || 'Consultando API...'}
                  </span>
                  <span style={{ fontWeight: 600 }}>{status?.progress || 0}%</span>
                </div>
                <div className="progress-track"><div className="progress-fill" style={{ width: `${status?.progress || 0}%` }} /></div>
                {allResults.length > 0 && <div className="progress-hint">{allResults.length} correspondência{allResults.length !== 1 ? 's' : ''} encontrada{allResults.length !== 1 ? 's' : ''} até agora</div>}
              </div>
            )}

            {!isProcessing && status?.status === 'cancelled' && (
              <div className="alert alert-amber" style={{ marginBottom: '1.25rem' }}>
                <AlertCircle size={14} style={{ flexShrink: 0 }} /> Consulta cancelada. Exibindo resultados parciais.
              </div>
            )}

            {!isProcessing && status?.status === 'completed' && allResults.length === 0 && (
              <div className="empty" style={{ color: 'var(--green)' }}>
                <CheckCircle2 size={40} style={{ opacity: 0.4, marginBottom: '0.25rem' }} />
                <p style={{ fontWeight: 600 }}>Nenhum servidor encontrado como beneficiário.</p>
                <p className="sub">Cruzamento concluído sem alertas.</p>
              </div>
            )}

            {allResults.length > 0 && (
              <>
                <div className="filter-wrap">
                  <Filter size={13} />
                  <input type="text" placeholder="Filtrar por nome, CPF ou município…" value={searchFilter} onChange={e => setSearchFilter(e.target.value)} />
                </div>

                {!agrupado && (
                  <div className="table-wrap">
                    <table>
                      <thead><tr>
                        <th style={{ width: 36 }}>#</th>
                        <th>Servidor</th><th>CPF</th><th>NIS</th><th>Beneficiário (API)</th>
                        <th>Município / UF</th><th>Mês Ref.</th><th>Data Saque</th><th>Valor</th>
                        {config.modo === 'municipio' && <th style={{ width: 56 }}>Pág.</th>}
                      </tr></thead>
                      <tbody>
                        {filteredResults.map((row, i) => (
                          <tr key={i}>
                            <td className="td-num">{i + 1}</td>
                            <td className="td-bold">{row.servidor}</td>
                            <td className="td-mono">{row.cpf}</td>
                            <td className="td-mono td-dim">{row.nis || '—'}</td>
                            <td>{row.beneficiario}</td>
                            <td className="td-dim">{row.municipio}{row.uf ? ` / ${row.uf}` : ''}</td>
                            <td className="td-dim">{fmtMes(row.mes)}</td>
                            <td className="td-dim">{row.data_saque || '—'}</td>
                            <td className="td-valor">R$ {(row.valor || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                            {config.modo === 'municipio' && <td className="td-num td-dim">{row.pagina ?? '—'}</td>}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {agrupado && (
                  <div className="table-wrap">
                    <table>
                      <thead><tr>
                        <th style={{ width: 28 }}></th>
                        <th>Servidor</th><th>CPF</th><th>NIS</th><th>Ocorrências</th><th>Meses</th><th>Valor Total</th>
                      </tr></thead>
                      <tbody>
                        {groupedResults.map(g => {
                          const expanded = expandedRows.has(g.cpf);
                          const mesesG = [...new Set(g.ocorrencias.map(o => o.mes))].sort().map(fmtMes);
                          const alerta = g.ocorrencias.length > 5;
                          return (
                            <React.Fragment key={g.cpf}>
                              <tr className={`row-group${expanded ? ' open' : ''}${alerta ? ' row-alert' : ''}`} onClick={() => toggleRow(g.cpf)}>
                                <td style={{ textAlign: 'center', color: 'var(--text-3)' }}>
                                  {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
                                </td>
                                <td className="td-bold">
                                  {alerta && <span className="badge badge-red" style={{ marginRight: '6px', fontSize: '0.65rem' }}>⚠ {g.ocorrencias.length}×</span>}
                                  {g.servidor}
                                </td>
                                <td className="td-mono">{g.cpf}</td>
                                <td className="td-mono td-dim">
                                  {g.nis || '—'}
                                  {g.nisCount > 1 && <span className="badge badge-amber" style={{ marginLeft: '6px', fontSize: '0.62rem' }} title={`${g.nisCount} NIS distintos — possível falso positivo`}>⚠ {g.nisCount} NIS</span>}
                                </td>
                                <td><span className={`badge-count${alerta ? ' badge-count-alert' : ''}`}>{g.ocorrencias.length}×</span></td>
                                <td className="td-dim">{mesesG.join(' · ')}</td>
                                <td className="td-valor">R$ {g.totalValor.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                              </tr>
                              {expanded && g.ocorrencias.map((o, i) => (
                                <tr key={i} className="row-sub">
                                  <td></td>
                                  <td colSpan={2} style={{ paddingLeft: '2rem', fontSize: '0.78rem', color: 'var(--text-2)' }}>{o.beneficiario || '—'}</td>
                                  <td className="td-mono td-dim" style={{ fontSize: '0.78rem' }}>{o.nis || '—'}</td>
                                  <td className="td-dim" style={{ fontSize: '0.78rem' }}>
                                    {o.municipio}{o.uf ? ` / ${o.uf}` : ''}
                                    {config.modo === 'municipio' && o.pagina != null && <span className="pagina-tag">p.{o.pagina}</span>}
                                  </td>
                                  <td className="td-dim" style={{ fontSize: '0.78rem' }}>{fmtMes(o.mes)} · {o.data_saque || '—'}</td>
                                  <td className="td-valor" style={{ fontSize: '0.78rem' }}>R$ {(o.valor || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
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
                  <p style={{ fontSize: '0.72rem', color: 'var(--text-3)', marginTop: '0.5rem', textAlign: 'right' }}>
                    {agrupado ? `${groupedResults.length} de ${uniqueServers} servidores` : `${filteredResults.length} de ${allResults.length} registros`}
                  </p>
                )}

                <div className="summary-bar">
                  <span>{uniqueServers} servidor{uniqueServers !== 1 ? 'es' : ''} único{uniqueServers !== 1 ? 's' : ''} · {allResults.length} ocorrência{allResults.length !== 1 ? 's' : ''}</span>
                  <span>Total: <strong>R$ {totalValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong></span>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
