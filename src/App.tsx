import { useState, useEffect, useMemo } from 'react';
import { Upload, FileJson, Table as TableIcon, Download, AlertCircle, Search, ShieldCheck, Filter, Columns, LayoutList } from 'lucide-react';

export default function App() {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<any>(null);
  const [data, setData] = useState<any[]>([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [search, setSearch] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [selectedTableGroup, setSelectedTableGroup] = useState<string>('All');
  const [tableViewMode, setTableViewMode] = useState<'horizontal' | 'vertical'>('vertical');

  // Debounce search
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(search);
      setPage(1); // Reset to page 1 on search
    }, 300);
    return () => clearTimeout(timer);
  }, [search]);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setFile(e.target.files[0]);
    }
  };

  const handleUpload = async () => {
    if (!file) return;
    setLoading(true);
    setError(null);
    
    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      });
      
      const contentType = res.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error(`Server error: ${res.status} ${res.statusText}. The server might be restarting or the file is too large.`);
      }

      const result = await res.json();
      if (!res.ok) throw new Error(result.error || 'Upload failed');
      
      setSummary(result.summary);
      setSearch(''); // Reset search on new upload
      setPage(1);
      setSelectedTableGroup('All');
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchData = async (p: number, s: string = debouncedSearch) => {
    try {
      const res = await fetch(`/api/data?page=${p}&limit=10&search=${encodeURIComponent(s)}`);
      const contentType = res.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Invalid response from server');
      }
      const result = await res.json();
      setData(result.data);
      setPage(result.page);
      setTotalPages(result.totalPages);
    } catch (err) {
      console.error('Failed to fetch data', err);
    }
  };

  useEffect(() => {
    if (summary) {
      fetchData(page, debouncedSearch);
    }
  }, [page, debouncedSearch, summary]);

  const handleExport = () => {
    if (!data || data.length === 0) return;
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'export.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const getScoreColor = (score: number) => {
    if (score >= 90) return 'text-green-600';
    if (score >= 70) return 'text-yellow-600';
    return 'text-red-600';
  };

  const semanticGroups = useMemo(() => {
    if (!summary) return ['All'];
    const groups = new Set(summary.columns.map((c: any) => c.semanticGroup));
    return ['All', ...Array.from(groups)] as string[];
  }, [summary]);

  const tableColumns = useMemo(() => {
    if (!summary) return [];
    if (selectedTableGroup === 'All') return summary.columns;
    return summary.columns.filter((c: any) => c.semanticGroup === selectedTableGroup);
  }, [summary, selectedTableGroup]);

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 font-sans">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <FileJson className="w-6 h-6 text-blue-600" />
          <h1 className="text-xl font-semibold tracking-tight">Data Ingestion & Analysis</h1>
        </div>
        {summary && (
          <button 
            onClick={handleExport}
            className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 text-sm font-medium rounded-md transition-colors"
          >
            <Download className="w-4 h-4" />
            Export JSON
          </button>
        )}
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8 space-y-8">
        
        {/* Upload Section */}
        <section className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm">
          <h2 className="text-lg font-medium mb-4">Upload Dataset</h2>
          <div className="flex items-center gap-4">
            <input 
              type="file" 
              accept=".json"
              onChange={handleFileChange}
              className="block w-full max-w-md text-sm text-gray-500
                file:mr-4 file:py-2 file:px-4
                file:rounded-md file:border-0
                file:text-sm file:font-medium
                file:bg-blue-50 file:text-blue-700
                hover:file:bg-blue-100 transition-colors"
            />
            <button 
              onClick={handleUpload}
              disabled={!file || loading}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-md disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {loading ? (
                <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              ) : (
                <Upload className="w-4 h-4" />
              )}
              Upload & Process
            </button>
          </div>
          {error && (
            <div className="mt-4 p-3 bg-red-50 text-red-700 rounded-md flex items-center gap-2 text-sm">
              <AlertCircle className="w-4 h-4" />
              {error}
            </div>
          )}
        </section>

        {summary && (
          <>
            {/* Summary Metrics */}
            <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm">
                <div className="text-sm text-gray-500 font-medium mb-1">Total Rows</div>
                <div className="text-3xl font-semibold">{summary.totalRows.toLocaleString()}</div>
              </div>
              <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm">
                <div className="text-sm text-gray-500 font-medium mb-1">Total Columns</div>
                <div className="text-3xl font-semibold">{summary.columns.length}</div>
              </div>
              <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm">
                <div className="flex items-center gap-2 text-sm text-gray-500 font-medium mb-1">
                  <ShieldCheck className="w-4 h-4" />
                  Data Quality Score
                </div>
                <div className={`text-3xl font-semibold ${getScoreColor(summary.qualityScore)}`}>
                  {summary.qualityScore}%
                </div>
                <div className="text-xs text-gray-400 mt-1">
                  Completeness: {summary.completeness}% | Consistency: {summary.consistency}%
                </div>
              </div>
            </section>

            {/* Main Content Area */}
            <section className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <div className="p-6">
                <div className="space-y-4">
                  {/* Toolbar: Search & Group Filter */}
                    <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
                      <div className="flex items-center gap-2 max-w-md w-full">
                        <div className="relative flex-1">
                          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                          <input 
                            type="text" 
                            placeholder="Search all columns..."
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            className="w-full pl-9 pr-4 py-2 border border-gray-300 rounded-md text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                          />
                        </div>
                      </div>
                      
                      <div className="flex items-center gap-2 overflow-x-auto pb-1 max-w-full">
                        {/* View Mode Toggle */}
                        <div className="flex items-center gap-1 bg-gray-100 p-1 rounded-md shrink-0">
                          <button
                            onClick={() => setTableViewMode('horizontal')}
                            className={`flex items-center gap-1 px-2 py-1 text-xs font-medium rounded-sm transition-colors ${tableViewMode === 'horizontal' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
                            title="Horizontal View"
                          >
                            <Columns className="w-3.5 h-3.5" />
                            Horizontal
                          </button>
                          <button
                            onClick={() => setTableViewMode('vertical')}
                            className={`flex items-center gap-1 px-2 py-1 text-xs font-medium rounded-sm transition-colors ${tableViewMode === 'vertical' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
                            title="Vertical View"
                          >
                            <LayoutList className="w-3.5 h-3.5" />
                            Vertical
                          </button>
                        </div>

                        <div className="w-px h-4 bg-gray-300 shrink-0 mx-1"></div>

                        <Filter className="w-4 h-4 text-gray-400 shrink-0" />
                        <span className="text-sm text-gray-500 font-medium shrink-0">View Group:</span>
                        {semanticGroups.map(group => (
                          <button
                            key={group}
                            onClick={() => setSelectedTableGroup(group)}
                            className={`px-3 py-1 text-xs font-medium rounded-full whitespace-nowrap transition-colors ${
                              selectedTableGroup === group 
                                ? 'bg-blue-100 text-blue-700 border border-blue-200' 
                                : 'bg-gray-100 text-gray-600 border border-gray-200 hover:bg-gray-200'
                            }`}
                          >
                            {group}
                          </button>
                        ))}
                      </div>
                    </div>

                    {tableViewMode === 'horizontal' ? (
                      <div className="overflow-x-auto border border-gray-200 rounded-lg">
                        <table className="w-full text-sm text-left">
                          <thead className="text-xs text-gray-500 uppercase bg-gray-50 border-b border-gray-200">
                            <tr>
                              {tableColumns.map((col: any) => (
                                <th key={col.name} className="px-4 py-3 font-medium whitespace-nowrap">
                                  {col.name}
                                  <span className="block text-[10px] text-gray-400 normal-case mt-0.5">{col.type}</span>
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {data.length > 0 ? (
                              data.map((row, i) => (
                                <tr key={i} className="border-b border-gray-100 hover:bg-gray-50">
                                  {tableColumns.map((col: any) => (
                                    <td key={col.name} className="px-4 py-3 whitespace-nowrap max-w-[200px] truncate">
                                      {row[col.name] !== null && row[col.name] !== undefined ? String(row[col.name]) : <span className="text-gray-300 italic">null</span>}
                                    </td>
                                  ))}
                                </tr>
                              ))
                            ) : (
                              <tr>
                                <td colSpan={tableColumns.length} className="px-4 py-8 text-center text-gray-500">
                                  No records found matching your search.
                                </td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="overflow-x-auto border border-gray-200 rounded-lg">
                        <table className="w-full text-sm text-left">
                          <thead className="text-xs text-gray-500 uppercase bg-gray-50 border-b border-gray-200">
                            <tr>
                              <th className="px-4 py-3 font-medium whitespace-nowrap bg-gray-50 sticky left-0 z-10 shadow-[1px_0_0_0_#e5e7eb]">Field Name</th>
                              <th className="px-4 py-3 font-medium whitespace-nowrap">Type</th>
                              {data.length > 0 ? data.map((_, i) => (
                                <th key={i} className="px-4 py-3 font-medium whitespace-nowrap min-w-[200px]">
                                  Record {(page - 1) * 10 + i + 1}
                                </th>
                              )) : (
                                <th className="px-4 py-3 font-medium whitespace-nowrap">Data</th>
                              )}
                            </tr>
                          </thead>
                          <tbody>
                            {data.length > 0 ? (
                              tableColumns.map((col: any) => (
                                <tr key={col.name} className="group border-b border-gray-100 hover:bg-gray-50">
                                  <td className="px-4 py-3 font-medium text-gray-900 whitespace-nowrap bg-white sticky left-0 z-10 shadow-[1px_0_0_0_#e5e7eb] group-hover:bg-gray-50">
                                    {col.name}
                                  </td>
                                  <td className="px-4 py-3 text-xs text-gray-500 whitespace-nowrap">
                                    {col.type}
                                  </td>
                                  {data.map((row, i) => (
                                    <td key={i} className="px-4 py-3 whitespace-nowrap max-w-[300px] truncate">
                                      {row[col.name] !== null && row[col.name] !== undefined ? String(row[col.name]) : <span className="text-gray-300 italic">null</span>}
                                    </td>
                                  ))}
                                </tr>
                              ))
                            ) : (
                              <tr>
                                <td colSpan={3} className="px-4 py-8 text-center text-gray-500">
                                  No records found matching your search.
                                </td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                    )}
                    
                    {/* Pagination */}
                    <div className="flex items-center justify-between pt-4 border-t border-gray-100">
                      <div className="text-sm text-gray-500">
                        Page {page} of {totalPages || 1}
                      </div>
                      <div className="flex gap-2">
                        <button 
                          onClick={() => setPage(Math.max(1, page - 1))}
                          disabled={page === 1}
                          className="px-3 py-1 border border-gray-200 rounded text-sm disabled:opacity-50 hover:bg-gray-50"
                        >
                          Previous
                        </button>
                        <button 
                          onClick={() => setPage(Math.min(totalPages, page + 1))}
                          disabled={page >= totalPages}
                          className="px-3 py-1 border border-gray-200 rounded text-sm disabled:opacity-50 hover:bg-gray-50"
                        >
                          Next
                        </button>
                      </div>
                    </div>
                  </div>
              </div>
            </section>
          </>
        )}
      </main>
    </div>
  );
}
