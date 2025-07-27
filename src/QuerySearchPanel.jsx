import React, {useState} from 'react';

const mockData = [
  { id: 1, lot: 'LotA', plan: 'PlanX' },
  { id: 2, lot: 'LotB', plan: 'PlanY' }
];

export default function QuerySearchPanel() {
  const [input, setInput] = useState('');
  const [results, setResults] = useState([]);

  const onSearch = async () => {
    await new Promise(r => setTimeout(r, 500));
    setResults(mockData);
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
