import React from 'react';

const tabs = [
  { id: 'map', label: 'Map' },
  { id: 'search', label: 'Search' }
];

export default function Tabs({ activeTab, onChange }) {
  return (
    <div className="flex border-b bg-gray-100">
      {tabs.map(t => (
        <button
          key={t.id}
          className={`px-4 py-2 focus:outline-none ${activeTab === t.id ? 'border-b-2 border-blue-500 font-semibold' : ''}`}
          onClick={() => onChange(t.id)}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}
