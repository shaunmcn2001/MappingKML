import React, { useState } from 'react';
import { dummySearch } from '../utils/dummySearch';

export default function SearchTab({ results, setResults }) {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSearch = async () => {
    setLoading(true);
    const res = await dummySearch(query);
    setResults(res);
    setLoading(false);
  };

  return (
    <div className="p-4 overflow-auto h-full">
      <textarea
        className="w-full border p-2"
        rows={4}
        placeholder="Lot/Plan one per line"
        value={query}
        onChange={e => setQuery(e.target.value)}
      />
      <button
        className="mt-2 px-4 py-2 bg-blue-500 text-white"
        onClick={handleSearch}
        disabled={loading}
      >
        {loading ? 'Searching...' : 'Search'}
      </button>
      <div className="mt-4">
        <table className="min-w-full border">
          <thead>
            <tr>
              <th className="border px-2">Lot</th>
              <th className="border px-2">Plan</th>
            </tr>
          </thead>
          <tbody>
            {results.map(r => (
              <tr key={r.id} className="hover:bg-gray-100 cursor-pointer">
                <td className="border px-2">{r.lot}</td>
                <td className="border px-2">{r.plan}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
