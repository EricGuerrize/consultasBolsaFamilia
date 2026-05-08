import React, { useState, useEffect, useRef, useMemo } from 'react';
import {
  Upload, Search, Download, AlertCircle, CheckCircle2,
  ShieldCheck, Loader2, Filter, FileText,
  ChevronDown, ChevronRight, RotateCcw, Users, X, Settings,
  Database, Layout, Zap, BarChart3, AlertTriangle,
} from 'lucide-react';

const PARALLEL_WORKERS = 1;

const MOCK_RESULTS = [
  {
    servidor: "MARIA SILVA SANTOS", cpf: "123.456.789-00", beneficiario: "MARIA SILVA SANTOS",
    cargo: "PROFESSOR", admissao: "10/02/2021", orgao: "PREFEITURA MUNICIPAL DE RONDONÓPOLIS",
    nis: "12345678901", municipio: "Rondonópolis", uf: "MT", mes: "202401", data_saque: "15/01/2024",
    valor: 600.00, tipo_ato: "NOMEAÇÃO", matricula: "10001", pagina: 1, isMatch: true, isIrregular: true
  },
  {
    servidor: "JOÃO PEDRO OLIVEIRA", cpf: "987.654.321-11", beneficiario: "JOÃO PEDRO OLIVEIRA",
    cargo: "VIGIA", admissao: "05/03/2023", orgao: "PREFEITURA MUNICIPAL DE RONDONÓPOLIS",
    nis: "10987654321", municipio: "Rondonópolis", uf: "MT", mes: "202402", data_saque: "18/02/2024",
    valor: 600.00, tipo_ato: "CONTRATAÇÃO TEMPORÁRIA", matricula: "10002", pagina: 1, isMatch: true, isIrregular: true
  },
  {
    servidor: "ANA COSTA PEREIRA", cpf: "456.789.123-22", beneficiario: "ANA COSTA PEREIRA",
    cargo: "MERENDEIRA", admissao: "01/04/2024", orgao: "PREFEITURA MUNICIPAL DE RONDONÓPOLIS",
    nis: "11223344556", municipio: "Rondonópolis", uf: "MT", mes: "202401", data_saque: "20/01/2024",
    valor: 600.00, tipo_ato: "NOMEAÇÃO", matricula: "10003", pagina: 1, isMatch: true, isIrregular: false
  },
  {
    servidor: "CARLOS ALBERTO SOUZA", cpf: "321.654.987-33", beneficiario: "CARLOS ALBERTO SOUZA",
    cargo: "MOTORISTA", admissao: "15/05/2018", orgao: "PREFEITURA MUNICIPAL DE RONDONÓPOLIS",
    nis: "66554433221", municipio: "Rondonópolis", uf: "MT", mes: "202403", data_saque: "10/03/2024",
    valor: 750.00, tipo_ato: "NOMEAÇÃO", matricula: "10004", pagina: 1, isMatch: true, isIrregular: true
  },
  {
    servidor: "BEATRIZ LIMA FERREIRA", cpf: "741.852.963-44", beneficiario: "BEATRIZ LIMA FERREIRA",
    cargo: "AUXILIAR ADMINISTRATIVO", admissao: "20/08/2022", orgao: "PREFEITURA MUNICIPAL DE RONDONÓPOLIS",
    nis: "99887766554", municipio: "Rondonópolis", uf: "MT", mes: "202402", data_saque: "22/02/2024",
    valor: 600.00, tipo_ato: "NOMEAÇÃO", matricula: "10005", pagina: 1, isMatch: true, isIrregular: true
  }
];

// YYYYMM ↔ YYYY-MM
const toMonthInput = m => m ? `${m.slice(0, 4)}-${m.slice(4)}` : '';
const fromMonthInput = v => v ? v.replace('-', '') : '';

export default function App() {
  // ── Fonte dos servidores ─────────────────────
  const [fonteServidores, setFonteServidores] = useState('csv');
  const [oracleConfig, setOracleConfig] = useState({ ent_codigo: '1118181', exercicio: '2024' });
  const [oracleInfo, setOracleInfo] = useState(null);

  // ── Arquivo CSV ──────────────────────────────
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
  const [municipioHighlight, setMunicipioHighlight] = useState(-1);
  const municipioRef = useRef(null);
  const municipioListRef = useRef(null);

  // ── Config consulta ──────────────────────────
  const [modoTeste, setModoTeste] = useState(true);
  const [modoApresentacao, setModoApresentacao] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showMapping, setShowMapping] = useState(false);
  const [config, setConfig] = useState({
    m_ini: '202401', m_fim: '202403',
    modo: 'municipio', ibge: '', api_key: '',
    col_cpf: '', col_nome: '', col_cargo: '', col_admissao: '',
  });
  const [baseUrl, setBaseUrl] = useState(localStorage.getItem('api_url') || '');

  // ── Resultados ───────────────────────────────
  const [fase, setFase] = useState('config');
  // viewMode: 'todos' | 'irregulares' | 'agrupado'
  const [viewMode, setViewMode] = useState('todos');
  const [expandedRows, setExpandedRows] = useState(new Set());
  const [paginaAtual, setPaginaAtual] = useState(1);
  const [itensPorPagina, setItensPorPagina] = useState(50);
  const cancelRef = useRef(false);

  useEffect(() => { localStorage.setItem('api_url', baseUrl); }, [baseUrl]);

  // ── Health checks ────────────────────────────
  useEffect(() => {
    const url = baseUrl ? `${baseUrl}/api/health` : '/api/health';
    fetch(url)
      .then(r => r.ok ? setApiHealth('ok') : setApiHealth('error'))
      .catch(() => setApiHealth('error'));
  }, [baseUrl]);

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
  const normalizarNome = nome => {
    if (!nome) return '';
    return String(nome).normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
  };
  const mascararCPF = cpfRaw => {
    const c = normalizarCPF(cpfRaw);
    if (c.length !== 11) return c;
    return `***.${c.slice(3, 6)}.${c.slice(6, 9)}-**`;
  };
  const chaveJS = (cpfRaw, nome) => {
    const nomeNorm = normalizarNome(nome);
    const cpfMasc = String(cpfRaw).includes('*') ? cpfRaw : mascararCPF(cpfRaw);
    return nomeNorm && cpfMasc ? `${nomeNorm}|${cpfMasc}` : '';
  };
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
    const detect = hdrs => {
      const find = (rx, blacklist = []) => hdrs.find(c => rx.test(c) && !blacklist.includes(c)) || '';
      const cpf = find(/pess_cpf|cpf|doc/i);
      const nome = find(/pess_nome|nome|servidor/i, [cpf]);
      const cargo = find(/desc_cargo|cargo|fun[cç][aã]o/i, [cpf, nome]);
      const adm = find(/dt_admissao|admiss[aã]o|contrat/i, [cpf, nome, cargo]);
      return { col_cpf: cpf, col_nome: nome, col_cargo: cargo, col_admissao: adm };
    };

    if (suffix === 'csv') {
      try {
        const { headers, total } = await parseCSV(f);
        setColumns(headers);
        setFileInfo({ total, filename: f.name });
        setConfig(prev => ({ ...prev, ...detect(headers) }));
      } catch (err) { setError('Erro ao ler CSV: ' + err.message); }
    } else {
      const fd = new FormData(); fd.append('file', f);
      try {
        const url = baseUrl ? `${baseUrl}/api/upload` : '/api/upload';
        const res = await fetch(url, { method: 'POST', body: fd });
        if (!res.ok) throw new Error('Erro ao ler arquivo');
        const data = await res.json();
        setColumns(data.columns);
        setFileInfo({ total: data.total, filename: data.filename });
        setConfig(prev => ({ ...prev, ...detect(data.columns) }));
      } catch (err) { setError(err.message); }
    }
  };

  const handleDrop = e => { e.preventDefault(); const f = e.dataTransfer.files[0]; if (f) handleFileUpload({ target: { files: [f] } }); };

  // ── Formato resultado ────────────────────────
  const formatResultJS = (srv, reg, pagina = null) => {
    const bf = reg.beneficiarioNovoBolsaFamilia || {};
    const mun = reg.municipio || {};
    const uf = mun.uf || {};

    // Portal às vezes inverte sigla e nome (ex: "Mato Grosso" em sigla)
    let ufAbbr = 'MT';
    const v1 = String(uf.sigla || ''), v2 = String(uf.nome || '');
    if (v1.length === 2) ufAbbr = v1;
    else if (v2.length === 2) ufAbbr = v2;
    else if (v1.toLowerCase().includes('mato grosso') || v2.toLowerCase().includes('mato grosso')) ufAbbr = 'MT';
    else ufAbbr = v1.slice(0, 2).toUpperCase() || v2.slice(0, 2).toUpperCase() || 'MT';

    const mesRef = (reg.dataMesReferencia || reg.mesReferencia || '').replace(/-/g, '').slice(0, 6);
    const admissao = srv.admissao || srv['Admissão'] || '';
    const isIrregular = (() => {
      if (!admissao || admissao.length < 10) return false;
      try {
        const [d, m, y] = admissao.split('/');
        return parseInt(`${y}${m}`) <= parseInt(mesRef);
      } catch { return false; }
    })();

    return {
      servidor: srv.nome || srv['Nome Servidor'] || '',
      cpf: srv.cpf || srv['CPF'] || '',
      beneficiario: bf.nome || '',
      cargo: srv.cargo || srv['Cargo'] || '',
      admissao,
      orgao: srv.orgao || srv['Órgão'] || '',
      nis: bf.nis || bf.ns || bf.numeroInscricaoSocial || '',
      municipio: mun.nomeIBGE || '', uf: ufAbbr,
      mes: mesRef,
      data_saque: reg.dataSaque || '', valor: reg.valorSaque ?? reg.valor ?? 0,
      tipo_ato: srv.tipo_ato || '',
      matricula: srv.pess_matricula || '',
      pagina,
      isIrregular,
    };
  };

  // ── Proxy API Portal da Transparência ────────
  const proxyFetch = async (endpoint, params, retries = 3) => {
    const url = baseUrl ? `${baseUrl}/api/proxy` : '/api/proxy';
    const res = await fetch(url, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint, params, api_key: config.api_key }),
    });
    if (res.status === 429) { await delay(2000); return proxyFetch(endpoint, params, retries); }
    if ((res.status === 502 || res.status === 504) && retries > 0) { await delay(3000); return proxyFetch(endpoint, params, retries - 1); }
    if (!res.ok) {
      if (res.status === 400 && endpoint === 'municipio') { console.warn('Portal API 400 — ignorando página.'); return []; }
      const t = await res.text(); let msg = t; try { msg = JSON.parse(t)?.detail || t; } catch {} throw new Error(msg);
    }
    return res.json();
  };

  // ── Constrói serverMap ───────────────────────
  const buildServerMap = async () => {
    const map = new Map();
    const add = (cpfRaw, nome, extra = {}) => {
      const cpf = normalizarCPF(cpfRaw);
      if (!cpf) return;
      const chave = chaveJS(cpf, nome);
      if (chave) { if (!map.has(chave)) map.set(chave, []); map.get(chave).push({ cpf, nome, ...extra }); }
    };

    if (fonteServidores === 'oracle') {
      setStatus(prev => ({ ...prev, message: 'Carregando servidores do Oracle...' }));
      const url = baseUrl ? `${baseUrl}/api/servidores` : '/api/servidores';
      const res = await fetch(url, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(oracleConfig),
      });
      if (!res.ok) { const t = await res.text(); throw new Error(JSON.parse(t)?.detail || t); }
      const { servidores, total } = await res.json();
      setOracleInfo({ total });
      servidores.forEach(s => add(s.cpf, s.nome, { cargo: s.cargo || '', admissao: s.admissao || '' }));
      return { serverMap: map, totalServidores: total };
    } else {
      const { rows, sep, headers } = await parseCSV(file);
      const idx = k => headers.indexOf(config[k]);
      const cpfIdx = idx('col_cpf'), nomeIdx = idx('col_nome');
      const cargoIdx = idx('col_cargo'), admIdx = idx('col_admissao');
      rows.slice(1).forEach(row => {
        const c = row.split(sep).map(v => v?.replace(/^"|"$/g, '') || '');
        add(c[cpfIdx], c[nomeIdx] || '', {
          cargo: cargoIdx >= 0 ? c[cargoIdx] : '',
          admissao: admIdx >= 0 ? c[admIdx] : '',
        });
      });
      return { serverMap: map, totalServidores: rows.length - 1 };
    }
  };

  // ── Cruzamento principal ──────────────────────
  const startCrossing = async () => {
    if (fonteServidores === 'csv' && !file) return;
    setLoading(true); setError(null);
    setStatus({ status: 'processing', progress: 0, result: [], message: 'Iniciando...' });
    setFase('processing'); setSearchFilter(''); setFase('processing'); setPaginaAtual(1); setExpandedRows(new Set());
    cancelRef.current = false;

    if (modoApresentacao) {
      setStatus(prev => ({ ...prev, message: 'Modo Apresentação Ativo. Buscando dados...' }));
      const delayMs = 6000;
      const steps = 100;
      for (let i = 0; i <= steps; i++) {
        if (cancelRef.current) break;
        await delay(delayMs / steps);
        setStatus(prev => ({ ...prev, progress: i }));
      }
      if (!cancelRef.current) {
        setStatus({ status: 'completed', progress: 100, result: MOCK_RESULTS, message: 'Concluído' });
        setFase('done');
      }
      setLoading(false);
      return;
    }

    try {
      const { serverMap } = await buildServerMap();
      if (!serverMap.size) throw new Error('Nenhum servidor carregado — verifique a fonte de dados.');

      const meses = getMesesList(config.m_ini, config.m_fim);
      const allResults = [];
      const seenResults = new Set();
      let mesesConcluidos = 0;

      const flush = () => setStatus(prev => ({ ...prev, result: [...allResults] }));

      if (config.modo === 'municipio') {
        const MAX_PAGINAS = modoTeste ? 100 : Infinity;

        const fetchMes = async mes => {
          if (cancelRef.current) return;
          let pagina = 1;
          while (true) {
            if (cancelRef.current) break;
            const regs = await proxyFetch('municipio', { mesAno: mes, codigoIbge: config.ibge, pagina });
            for (const reg of regs) {
              const bf = reg.beneficiarioNovoBolsaFamilia || {};
              const chave = chaveJS(bf.cpfFormatado || '', bf.nome || '');
              const isMatch = chave && serverMap.has(chave);

              if (isMatch) {
                for (const srv of serverMap.get(chave)) {
                  const deduKey = `${srv.cpf}|${mes}|${reg.dataSaque || ''}|${reg.valorSaque ?? reg.valor ?? 0}`;
                  if (seenResults.has(deduKey)) continue;
                  seenResults.add(deduKey);
                  allResults.push({ ...formatResultJS(srv, reg, pagina), isMatch: true });
                }
              } else {
                const deduKey = `no-match|${bf.nis}|${mes}|${reg.dataSaque || ''}|${reg.valorSaque ?? reg.valor ?? 0}`;
                if (seenResults.has(deduKey)) continue;
                seenResults.add(deduKey);
                allResults.push({ ...formatResultJS({ nome: bf.nome, cpf: bf.cpfFormatado }, reg, pagina), isMatch: false });
              }
            }
            flush();
            if (regs.length < 15 || pagina >= MAX_PAGINAS) break;
            pagina++;
            await delay(300);
          }
          mesesConcluidos++;
          setStatus(prev => ({
            ...prev,
            progress: Math.round((mesesConcluidos / meses.length) * 100),
            message: `${mesesConcluidos}/${meses.length} meses concluídos`,
          }));
        };

        for (let i = 0; i < meses.length; i += PARALLEL_WORKERS) {
          if (cancelRef.current) break;
          const lote = meses.slice(i, i + PARALLEL_WORKERS);
          setStatus(prev => ({ ...prev, message: `Buscando: ${lote.map(fmtMes).join(' · ')}` }));
          await Promise.all(lote.map(mes => fetchMes(mes)));
        }
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
                allResults.push({ ...formatResultJS(srv, reg), isMatch: true });
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
    setViewMode('todos'); setExpandedRows(new Set()); setPaginaAtual(1); setLoading(false);
    setOracleInfo(null);
  };

  // ── Dados derivados ──────────────────────────
  const allResults = status?.result || [];
  const matchResults = useMemo(() => allResults.filter(r => r.isMatch), [allResults]);

  const filteredResults = useMemo(() => {
    let base = viewMode === 'irregulares' ? matchResults.filter(r => r.isIrregular) : matchResults;
    if (!searchFilter) return base;
    const q = searchFilter.toLowerCase();
    return base.filter(r =>
      r.servidor?.toLowerCase().includes(q) ||
      r.cpf?.includes(q) ||
      r.municipio?.toLowerCase().includes(q) ||
      r.beneficiario?.toLowerCase().includes(q)
    );
  }, [matchResults, searchFilter, viewMode]);

  const totalValue    = useMemo(() => matchResults.reduce((s, r) => s + (r.valor || 0), 0), [matchResults]);
  const uniqueServers = useMemo(() => new Set(matchResults.map(r => r.cpf)).size, [matchResults]);
  const irrCount      = useMemo(() => new Set(matchResults.filter(r => r.isIrregular).map(r => r.cpf)).size, [matchResults]);
  const irrValue      = useMemo(() => matchResults.filter(r => r.isIrregular).reduce((s, r) => s + (r.valor || 0), 0), [matchResults]);

  const groupedAll = useMemo(() => {
    const map = new Map();
    for (const r of filteredResults) {
      const gKey = `${r.cpf}|${r.servidor}`;
      if (!map.has(gKey)) map.set(gKey, { servidor: r.servidor, cpf: r.cpf, matricula: r.matricula, nis: r.nis || '', nisSet: new Set(), ocorrencias: [], totalValor: 0, isIrregular: false });
      const g = map.get(gKey);
      if (!g.nis && r.nis) g.nis = r.nis;
      if (r.nis) g.nisSet.add(r.nis);
      g.ocorrencias.push(r);
      g.totalValor += r.valor || 0;
      if (r.isIrregular) g.isIrregular = true;
    }
    return [...map.values()]
      .map(g => ({ ...g, nisCount: g.nisSet.size }))
      .sort((a, b) => b.ocorrencias.length - a.ocorrencias.length || b.totalValor - a.totalValor);
  }, [filteredResults]);

  const isGrouped = viewMode === 'agrupado' || viewMode === 'irregulares';
  const displayList = isGrouped ? groupedAll : filteredResults;

  const totalPaginas = Math.ceil(displayList.length / itensPorPagina);
  const paginatedItems = useMemo(() => {
    const start = (paginaAtual - 1) * itensPorPagina;
    return displayList.slice(start, start + itensPorPagina);
  }, [displayList, paginaAtual, itensPorPagina]);

  const toggleRow = gKey => setExpandedRows(prev => { const next = new Set(prev); next.has(gKey) ? next.delete(gKey) : next.add(gKey); return next; });

  const exportCSV = () => {
    if (!matchResults.length) return;
    const hdrs = ['Servidor', 'CPF', 'Matrícula', 'NIS', 'Beneficiário', 'Município', 'UF', 'Mês Ref.', 'Data Saque', 'Valor', 'Irregular', 'Página'];
    const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const csv = '﻿' + [hdrs.join(','), ...matchResults.map(r => [r.servidor, r.cpf, r.matricula, r.nis, r.beneficiario, r.municipio, r.uf, r.mes, r.data_saque, r.valor, r.isIrregular ? 'SIM' : 'NÃO', r.pagina ?? ''].map(esc).join(','))].join('\n');
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8;' }));
    Object.assign(document.createElement('a'), { href: url, download: 'resultados_bolsafamilia.csv' }).click();
    URL.revokeObjectURL(url);
  };

  const canStart = !loading && (config.modo !== 'municipio' || config.ibge) && (
    fonteServidores === 'oracle' ? true : (file && config.col_cpf)
  );

  const labelPeriodo = `${fmtMes(config.m_ini)} – ${fmtMes(config.m_fim)}`;
  const labelLocal   = config.modo === 'municipio' ? (municipioSearch || config.ibge || '—') : 'Por CPF';
  const labelFonte   = fonteServidores === 'oracle'
    ? `Oracle · entidade ${oracleConfig.ent_codigo} / ${oracleConfig.exercicio}`
    : (fileInfo?.filename || 'CSV');
  const isProcessing = fase === 'processing';

  // ═══════════════════════════════════════════════════════
  // INFO BOX (reutilizado em config + resultados)
  // ═══════════════════════════════════════════════════════
  const InfoBox = () => (
    <div className="info-box-modern">
      <h2 className="info-main-title">Como funciona este monitor de auditoria</h2>
      <div className="info-grid-modern">
        <div className="info-col">
          <div className="info-section-header"><Layout size={18} /><h3>O que significa cada filtro</h3></div>
          <div className="info-list">
            <div className="info-list-item">
              <span className="dot-indicator blue"></span>
              <div><strong>Todos</strong><p>Servidores com qualquer registro de recebimento no Bolsa Família no período selecionado.</p></div>
            </div>
            <div className="info-list-item">
              <span className="dot-indicator red"></span>
              <div><strong>Irregulares</strong><p>Casos onde o saque ocorreu <strong>após ou no mesmo mês</strong> da admissão no cargo público — o caso mais suspeito.</p></div>
            </div>
            <div className="info-list-item">
              <span className="dot-indicator blue"></span>
              <div><strong>Por servidor</strong><p>Visão consolidada: agrupa todos os saques de um mesmo servidor em uma linha expansível, com total acumulado.</p></div>
            </div>
          </div>
        </div>
        <div className="info-col">
          <div className="info-section-header"><Zap size={18} /><h3>Como o cruzamento é feito</h3></div>
          <p className="info-text">O sistema realiza o cruzamento em 3 etapas:</p>
          <ol className="info-steps">
            <li><strong>Identificação:</strong> Extrai CPF e data de admissão do Oracle ou CSV.</li>
            <li><strong>Pareamento:</strong> Compara nome normalizado e CPF mascarado com o Portal da Transparência.</li>
            <li><strong>Validação Temporal:</strong> Verifica se o saque coincide com período em que o servidor já era ativo.</li>
          </ol>
        </div>
      </div>

      <div className="info-grid-modern" style={{ marginTop: '2rem' }}>
        <div className="info-col">
          <div className="info-section-header"><Database size={18} /><h3>Os sistemas monitorados</h3></div>
          <ul className="info-bullets">
            <li><strong>Oracle Municipal:</strong> Dados de pessoal, cargos e datas de ingresso.</li>
            <li><strong>Portal da Transparência:</strong> Pagamentos do Novo Bolsa Família (Governo Federal).</li>
          </ul>
          <div className="info-note">* Busca em tempo real via API oficial do Governo Federal.</div>
        </div>
        <div className="info-col">
          <div className="info-section-header"><BarChart3 size={18} /><h3>Atenção: falsos positivos</h3></div>
          <p className="info-text">Nem todo match é uma irregularidade confirmada. Fique atento a:</p>
          <ul className="info-bullets">
            <li><strong>⚠ X NIS:</strong> NIS distintos para o mesmo CPF indicam que o beneficiário pode ser outra pessoa com nome semelhante.</li>
            <li><strong>Modo Teste ativo:</strong> Limita a 100 páginas/mês e 500 CPFs — resultados podem estar incompletos.</li>
            <li><strong>NIS</strong> (Número de Identificação Social) é diferente do CPF — identifica o beneficiário no Cadastro Único.</li>
          </ul>
        </div>
      </div>

      <div className="info-footer-modern">
        Este monitor é uma ferramenta de apoio à fiscalização. Todo alerta deve ser submetido a processo administrativo para ampla defesa e contraditório antes de conclusões definitivas.
      </div>
    </div>
  );

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
                  <button className={`source-btn ${fonteServidores === 'oracle' ? 'active' : ''}`} onClick={() => setFonteServidores('oracle')}>
                    <Database size={13} /> Oracle
                  </button>
                  <button className={`source-btn ${fonteServidores === 'csv' ? 'active' : ''}`} onClick={() => setFonteServidores('csv')}>
                    <Upload size={13} /> CSV / Excel
                  </button>
                </div>
              </div>

              {/* ── Oracle: campos diretos (sem collapsible) ── */}
              {fonteServidores === 'oracle' && (
                <div className="mapping-box" style={{ marginBottom: '1.1rem' }}>
                  <div className="mapping-title" style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                    <Database size={11} /> Conexão Oracle
                  </div>
                  <div className="field-row">
                    <div className="field" style={{ marginBottom: 0 }}>
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
                    <div style={{ marginTop: '0.5rem' }}>
                      <button type="button" className="advanced-toggle" style={{ fontSize: '0.68rem', opacity: 0.8 }} onClick={() => setShowMapping(a => !a)}>
                        <Settings size={10} /> {showMapping ? 'Ocultar mapeamento' : 'Ajustar mapeamento de colunas'}
                      </button>
                      {showMapping && (
                        <div className="mapping-box fade-in" style={{ marginTop: '0.5rem' }}>
                          {[['col_cpf', 'Coluna de CPF *'], ['col_nome', 'Coluna de Nome'], ['col_cargo', 'Coluna de Cargo'], ['col_admissao', 'Coluna de Admissão']].map(([key, lbl], idx, arr) => (
                            <div className="field" key={key} style={{ marginBottom: idx < arr.length - 1 ? '0.7rem' : 0 }}>
                              <label>{lbl}</label>
                              <select value={config[key]} onChange={e => setConfig({ ...config, [key]: e.target.value })}>
                                <option value="">— Selecione —</option>
                                {columns.map(c => <option key={c} value={c}>{c}</option>)}
                              </select>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </>
              )}

              {/* ── Período via input[type=month] ── */}
              <div className="field" style={{ marginTop: '1.1rem' }}>
                <label>Período de referência</label>
                <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'flex-end' }}>
                  <div className="field" style={{ marginBottom: 0, flex: 1, minWidth: 0 }}>
                    <label style={{ fontWeight: 400, fontSize: '0.7rem', color: 'var(--text-3)' }}>De</label>
                    <div className="month-input-wrap">
                      <input type="month" value={toMonthInput(config.m_ini)}
                        onChange={e => setConfig({ ...config, m_ini: fromMonthInput(e.target.value) })} />
                    </div>
                  </div>
                  <div style={{ color: 'var(--text-3)', paddingBottom: '0.65rem', fontSize: '0.9rem', flexShrink: 0 }}>→</div>
                  <div className="field" style={{ marginBottom: 0, flex: 1, minWidth: 0 }}>
                    <label style={{ fontWeight: 400, fontSize: '0.7rem', color: 'var(--text-3)' }}>Até</label>
                    <div className="month-input-wrap">
                      <input type="month" value={toMonthInput(config.m_fim)}
                        onChange={e => setConfig({ ...config, m_fim: fromMonthInput(e.target.value) })} />
                    </div>
                  </div>
                </div>
              </div>

              {/* ── Modo ── */}
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
                    {(() => {
                      const q = municipioSearch.toLowerCase();
                      const filtered = municipioSearch.trim().length >= 2
                        ? municipios.filter(m => m.uf === 'MT' && (m.nome.toLowerCase().includes(q) || m.id.includes(municipioSearch))).slice(0, 50)
                        : [];

                      const selectItem = m => {
                        setConfig(p => ({ ...p, ibge: m.id }));
                        setMunicipioSearch(m.nome);
                        setShowMunicipioDropdown(false);
                        setMunicipioHighlight(-1);
                      };

                      const handleKeyDown = e => {
                        if (!showMunicipioDropdown || !filtered.length) return;
                        if (e.key === 'ArrowDown') {
                          e.preventDefault();
                          setMunicipioHighlight(h => {
                            const next = Math.min(h + 1, filtered.length - 1);
                            const el = municipioListRef.current?.children[next];
                            el?.scrollIntoView({ block: 'nearest' });
                            return next;
                          });
                        } else if (e.key === 'ArrowUp') {
                          e.preventDefault();
                          setMunicipioHighlight(h => {
                            const next = Math.max(h - 1, 0);
                            const el = municipioListRef.current?.children[next];
                            el?.scrollIntoView({ block: 'nearest' });
                            return next;
                          });
                        } else if (e.key === 'Enter' && municipioHighlight >= 0) {
                          e.preventDefault();
                          selectItem(filtered[municipioHighlight]);
                        } else if (e.key === 'Escape') {
                          setShowMunicipioDropdown(false);
                          setMunicipioHighlight(-1);
                        }
                      };

                      return (
                        <>
                          <input type="text" className="input-large"
                            placeholder={municipios.length === 0 ? 'Carregando municípios...' : 'Digite o nome do município...'}
                            value={municipioSearch} disabled={municipios.length === 0}
                            onChange={e => {
                              setMunicipioSearch(e.target.value);
                              setShowMunicipioDropdown(true);
                              setMunicipioHighlight(-1);
                              if (!e.target.value) setConfig(p => ({ ...p, ibge: '' }));
                            }}
                            onFocus={() => setShowMunicipioDropdown(true)}
                            onKeyDown={handleKeyDown} />
                          {config.ibge && <span className="combobox-badge">{config.ibge}</span>}
                          {showMunicipioDropdown && filtered.length > 0 && (
                            <ul className="combobox-dropdown dropdown-large" ref={municipioListRef}>
                              {filtered.map((m, idx) => (
                                <li key={m.id}
                                  className={[config.ibge === m.id ? 'active' : '', idx === municipioHighlight ? 'highlighted' : ''].join(' ').trim()}
                                  onMouseEnter={() => setMunicipioHighlight(idx)}
                                  onMouseDown={() => selectItem(m)}>
                                  <span className="city-name" style={{ fontWeight: 600 }}>{m.nome}</span>
                                  <span className="city-meta">{m.id}</span>
                                </li>
                              ))}
                            </ul>
                          )}
                          {showMunicipioDropdown && municipioSearch.trim().length >= 2 && filtered.length === 0 && (
                            <ul className="combobox-dropdown dropdown-large">
                              <li className="city-empty">Nenhum município encontrado em MT</li>
                            </ul>
                          )}
                        </>
                      );
                    })()}
                  </div>
                </div>
              )}

              <div className="divider" />

              {/* ── Avançado (API key + URL) ── */}
              <button type="button" className="advanced-toggle" onClick={() => setShowAdvanced(a => !a)}>
                <ChevronRight size={11} style={{ transform: showAdvanced ? 'rotate(90deg)' : 'none', transition: 'transform 0.2s' }} />
                <Settings size={11} /> Configurações avançadas
              </button>
              {showAdvanced && (
                <>
                  <div className="field" style={{ marginBottom: '0.6rem' }}>
                    <label>Chave de API (sobrepõe a do servidor)</label>
                    <input type="password" placeholder="Chave do Portal da Transparência" value={config.api_key} onChange={e => setConfig({ ...config, api_key: e.target.value })} />
                  </div>
                  <div className="field" style={{ marginBottom: '0.75rem' }}>
                    <label>URL da API (local ou ngrok)</label>
                    <input type="text" placeholder="http://localhost:8000" value={baseUrl} onChange={e => setBaseUrl(e.target.value)} />
                    <p style={{ fontSize: '0.65rem', color: 'var(--text-3)', marginTop: '6px', lineHeight: '1.4' }}>
                      Deixe em branco para usar o proxy padrão. Cole o link do ngrok para acesso remoto.
                    </p>
                  </div>
                </>
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

        <InfoBox />

        <div style={{ position: 'fixed', bottom: 10, right: 10, opacity: 0.3, transition: 'opacity 0.2s', zIndex: 100 }} onMouseEnter={e => e.currentTarget.style.opacity = 1} onMouseLeave={e => e.currentTarget.style.opacity = 0.3}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '10px' }}>
            <input type="checkbox" checked={modoApresentacao} onChange={e => setModoApresentacao(e.target.checked)} id="mockToggle" style={{ width: 'auto', margin: 0 }} />
            <label htmlFor="mockToggle" style={{ cursor: 'pointer', margin: 0, fontWeight: 'normal', color: 'var(--text-3)' }}>Modo Demo</label>
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
        {/* ── Stat cards ── */}
        <div className="stats-row cols-4" style={{ marginTop: '1.5rem' }}>
          <div className="stat-card">
            <div className="stat-label">Servidores com ocorrência</div>
            <div className={`stat-value${uniqueServers > 0 ? ' danger' : ''}`}>{uniqueServers.toLocaleString('pt-BR')}</div>
            <div className="stat-sub">encontrados no Bolsa Família</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Total de registros</div>
            <div className={`stat-value${matchResults.length > 0 ? ' danger' : ''}`}>{matchResults.length.toLocaleString('pt-BR')}</div>
            <div className="stat-sub">saques identificados</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Irregularidades</div>
            <div className={`stat-value${irrCount > 0 ? ' danger' : ' muted'}`}>{irrCount > 0 ? irrCount.toLocaleString('pt-BR') : isProcessing ? '…' : '—'}</div>
            <div className="stat-sub">servidores com saque indevido</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Valor em risco</div>
            <div className={`stat-value muted${irrValue > 0 ? ' danger' : ''}`} style={{ fontSize: '1.25rem' }}>
              {irrValue > 0 ? `R$ ${irrValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}` : isProcessing ? '…' : '—'}
            </div>
            <div className="stat-sub">soma dos casos irregulares · {labelPeriodo}</div>
          </div>
        </div>

        {/* ── Banner de config ── */}
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

            {matchResults.length > 0 && (
              <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', flexWrap: 'wrap' }}>
                <div className="view-tabs">
                  <button className={`view-tab ${viewMode === 'todos' ? 'active' : ''}`}
                    onClick={() => { setViewMode('todos'); setExpandedRows(new Set()); setPaginaAtual(1); }}>
                    Todos
                    <span className="view-tab-count">{matchResults.length}</span>
                  </button>
                  <button className={`view-tab ${viewMode === 'irregulares' ? 'active active-red' : ''}`}
                    onClick={() => { setViewMode('irregulares'); setExpandedRows(new Set()); setPaginaAtual(1); }}
                    title="Servidores com saque após data de admissão">
                    <AlertTriangle size={11} /> Irregulares
                    {irrCount > 0 && <span className="view-tab-count red">{irrCount}</span>}
                  </button>
                  <button className={`view-tab ${viewMode === 'agrupado' ? 'active' : ''}`}
                    onClick={() => { setViewMode('agrupado'); setExpandedRows(new Set()); setPaginaAtual(1); }}>
                    <Users size={11} /> Por servidor
                    <span className="view-tab-count">{uniqueServers}</span>
                  </button>
                </div>
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
                {matchResults.length > 0 && <div className="progress-hint">{matchResults.length} correspondência{matchResults.length !== 1 ? 's' : ''} encontrada{matchResults.length !== 1 ? 's' : ''} até agora</div>}
              </div>
            )}

            {!isProcessing && status?.status === 'cancelled' && (
              <div className="alert alert-amber" style={{ marginBottom: '1.25rem' }}>
                <AlertCircle size={14} style={{ flexShrink: 0 }} /> Consulta cancelada. Exibindo resultados parciais.
              </div>
            )}

            {!isProcessing && status?.status === 'completed' && matchResults.length === 0 && (
              <div className="empty" style={{ color: 'var(--green)' }}>
                <CheckCircle2 size={40} style={{ opacity: 0.4, marginBottom: '0.25rem' }} />
                <p style={{ fontWeight: 600 }}>Nenhum servidor encontrado como beneficiário.</p>
                <p className="sub">Cruzamento concluído sem alertas.</p>
              </div>
            )}

            {matchResults.length > 0 && (
              <>
                <div className="filter-wrap">
                  <Filter size={13} />
                  <input type="text" placeholder="Filtrar por nome, CPF ou município…" value={searchFilter}
                    onChange={e => { setSearchFilter(e.target.value); setPaginaAtual(1); }} />
                </div>

                {/* ── Vista flat (Todos) ── */}
                {!isGrouped && (
                  <div className="table-wrap">
                    <table>
                      <thead><tr>
                        <th style={{ width: 36 }}>#</th>
                        <th>Servidor</th><th>CPF</th><th>Beneficiário (API)</th>
                        <th>Município / UF</th><th>Mês Ref.</th><th>Valor</th>
                        {config.modo === 'municipio' && <th style={{ width: 56 }}>Pág.</th>}
                      </tr></thead>
                      <tbody>
                        {paginatedItems.map((row, i) => (
                          <tr key={i}>
                            <td className="td-num">{(paginaAtual - 1) * itensPorPagina + i + 1}</td>
                            <td className="td-bold">
                              {row.servidor}
                              {row.isIrregular && <span className="label-tag" style={{ marginLeft: 8, background: 'rgba(239,68,68,0.1)', color: '#ef4444', border: '1px solid rgba(239,68,68,0.2)' }}>IRREGULAR</span>}
                            </td>
                            <td className="td-mono">
                              <div>{row.cpf}</div>
                              {row.matricula && <div style={{ fontSize: '0.65rem', opacity: 0.7 }}>Mat: {row.matricula}</div>}
                            </td>
                            <td>{row.beneficiario}</td>
                            <td className="td-dim">{row.municipio}{row.uf ? ` / ${row.uf}` : ''}</td>
                            <td className="td-dim">{fmtMes(row.mes)}</td>
                            <td className="td-valor">R$ {(row.valor || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                            {config.modo === 'municipio' && <td className="td-num td-dim">{row.pagina ?? '—'}</td>}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* ── Vista agrupada (Agrupado + Irregulares) ── */}
                {isGrouped && (
                  <>
                    {groupedAll.length === 0 && (
                      <div className="empty" style={{ color: 'var(--green)', padding: '2.5rem' }}>
                        <CheckCircle2 size={32} style={{ opacity: 0.4, marginBottom: '0.25rem' }} />
                        <p style={{ fontWeight: 600 }}>
                          {viewMode === 'irregulares' ? 'Nenhuma irregularidade detectada.' : 'Sem resultados para agrupar.'}
                        </p>
                      </div>
                    )}
                    {groupedAll.length > 0 && (
                      <div className="table-wrap">
                        <table>
                          <thead><tr>
                            <th style={{ width: 28 }}></th>
                            <th>Servidor</th><th>CPF</th><th>NIS</th>
                            <th>Ocorrências</th><th>Meses</th><th>Valor Total</th>
                          </tr></thead>
                          <tbody>
                            {paginatedItems.map(g => {
                              const gKey = `${g.cpf}|${g.servidor}`;
                              const expanded = expandedRows.has(gKey);
                              const mesesUnicos = [...new Set(g.ocorrencias.map(o => o.mes))].sort();
                              const mesesLabel = mesesUnicos.length <= 3
                                ? mesesUnicos.map(fmtMes).join(' · ')
                                : `${fmtMes(mesesUnicos[0])} – ${fmtMes(mesesUnicos[mesesUnicos.length - 1])} (${mesesUnicos.length} meses)`;
                              return (
                                <React.Fragment key={gKey}>
                                  <tr className={`row-group${expanded ? ' open' : ''}${g.isIrregular ? ' row-alert' : ''}`} onClick={() => toggleRow(gKey)}>
                                    <td style={{ textAlign: 'center', color: 'var(--text-3)' }}>
                                      {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
                                    </td>
                                    <td className="td-bold">
                                      {g.isIrregular && <span className="badge badge-red" style={{ marginRight: '6px', fontSize: '0.65rem' }}>⚠ IRREGULAR</span>}
                                      {g.servidor}
                                    </td>
                                    <td className="td-mono">
                                      <div>{g.cpf}</div>
                                      {g.matricula && <div style={{ fontSize: '0.65rem', opacity: 0.7 }}>Mat: {g.matricula}</div>}
                                    </td>
                                    <td className="td-mono td-dim">
                                      {g.nis || '—'}
                                      {g.nisCount > 1 && <span className="badge badge-amber" style={{ marginLeft: '6px', fontSize: '0.62rem' }} title={`${g.nisCount} NIS distintos`}>⚠ {g.nisCount} NIS</span>}
                                    </td>
                                    <td><span className={`badge-count${g.isIrregular ? ' badge-count-alert' : ''}`}>{g.ocorrencias.length}×</span></td>
                                    <td className="td-dim" style={{ fontSize: '0.78rem' }}>{mesesLabel}</td>
                                    <td className="td-valor">R$ {g.totalValor.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</td>
                                  </tr>
                                  {expanded && g.ocorrencias.map((o, i) => (
                                    <tr key={i} className="row-sub">
                                      <td></td>
                                      <td colSpan={2} style={{ paddingLeft: '2rem', fontSize: '0.78rem', color: 'var(--text-2)' }}>
                                        {o.beneficiario || '—'}
                                        {o.isIrregular && o.admissao && (
                                          <div style={{ fontSize: '0.68rem', color: 'var(--red)', marginTop: '2px' }}>
                                            Admissão: {o.admissao} · Saque em {fmtMes(o.mes)}
                                          </div>
                                        )}
                                      </td>
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
                  </>
                )}

                {searchFilter && (
                  <p style={{ fontSize: '0.72rem', color: 'var(--text-3)', marginTop: '0.5rem', textAlign: 'right' }}>
                    {isGrouped ? `${groupedAll.length} de ${uniqueServers} servidores` : `${filteredResults.length} de ${matchResults.length} registros`}
                  </p>
                )}

                <div className="summary-bar">
                  <span>{uniqueServers} servidor{uniqueServers !== 1 ? 'es' : ''} · {matchResults.length} ocorrência{matchResults.length !== 1 ? 's' : ''} · {irrCount} irregular{irrCount !== 1 ? 'es' : ''}</span>
                  <span>Total: <strong>R$ {totalValue.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong></span>
                </div>

                {/* ── Paginação ── */}
                {totalPaginas > 1 && (
                  <div className="pagination">
                    <div className="pagination-controls">
                      <span>Exibir:</span>
                      <select className="pagination-select" value={itensPorPagina}
                        onChange={e => { setItensPorPagina(Number(e.target.value)); setPaginaAtual(1); }}>
                        <option value={25}>25 por página</option>
                        <option value={50}>50 por página</option>
                        <option value={100}>100 por página</option>
                      </select>
                    </div>
                    <div className="pagination-nav">
                      <button className="pagination-btn" disabled={paginaAtual === 1}
                        onClick={() => { setPaginaAtual(p => p - 1); window.scrollTo(0, 0); }}>Anterior</button>
                      <span className="pagination-pages">Página <strong>{paginaAtual}</strong> de {totalPaginas}</span>
                      <button className="pagination-btn" disabled={paginaAtual >= totalPaginas}
                        onClick={() => { setPaginaAtual(p => p + 1); window.scrollTo(0, 0); }}>Próxima</button>
                    </div>
                  </div>
                )}

                <InfoBox />
              </>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
