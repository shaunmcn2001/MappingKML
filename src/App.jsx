import React, {useState} from 'react';
import Tabs from './components/Tabs';
import MapPanel from './components/MapPanel';
import SearchTab from './components/SearchTab';

export default function App() {
  const [activeTab, setActiveTab] = useState('map');
  const [searchResults, setSearchResults] = useState([]);

  return (
    <div className="flex flex-col w-full h-screen">
      <Tabs activeTab={activeTab} onChange={setActiveTab} />
      <div className="flex-1 overflow-hidden">
        {activeTab === 'search' ? (
          <SearchTab results={searchResults} setResults={setSearchResults} />
        ) : (
          <MapPanel highlighted={searchResults} />
        )}
      </div>
    </div>
  );
}
