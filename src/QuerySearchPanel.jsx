import React, {useState} from 'react';

export default function QuerySearchPanel() {
  const [input, setInput] = useState('');
  const [results, setResults] = useState([]);

  const onSearch = async () => {
    const queries = input.split(/\n/).map(l => l.trim()).filter(Boolean);
    if (queries.length === 0) return;
    try {
      const res = await fetch('/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ queries })
      });
      const data = await res.json();
      const features = data.features || [];
      const regions = data.regions || [];
      const rows = features.map((f, idx) => {
        const props = f.properties || {};
        if (regions[idx] === 'QLD') {
          return { id: idx + 1, lot: props.lot, plan: props.plan };
        }
        return {
          id: idx + 1,
          lot: props.lotnumber,
          plan: props.planlabel || ''
        };
      });
      setResults(rows);
    } catch (err) {
      console.error(err);
      setResults([]);
    }
  };

  return (
    <div style={{padding: '1rem'}}>
      <h3>Lot / Plan Search</h3>
      <textarea
        rows={4}
        style={{width: '100%'}}
        placeholder="One lot/plan per line"
        value={input}
        onChange={e => setInput(e.target.value)}
      />
      <button onClick={onSearch} style={{marginTop: '0.5rem'}}>Search</button>
      <div style={{marginTop: '1rem', maxHeight: '200px', overflowY: 'auto'}}>
        {results.length > 0 ? (
          <table>
            <thead>
              <tr><th>ID</th><th>Lot</th><th>Plan</th></tr>
            </thead>
            <tbody>
              {results.map(r =>
                <tr key={r.id}>
                  <td>{r.id}</td><td>{r.lot}</td><td>{r.plan}</td>
                </tr>
              )}
            </tbody>
          </table>
        ) : <div>No results yet.</div>}
      </div>
    </div>
  );
}
