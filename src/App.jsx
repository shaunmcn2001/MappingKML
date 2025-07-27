import React from 'react';
import KeplerGl from 'kepler.gl';
import keplerPlugin from './keplerPlugin';

function App() {
  return (
    <div style={{position: 'absolute', width: '100%', height: '100%'}}>
      <KeplerGl
        id="map"
        mapboxApiAccessToken={process.env.REACT_APP_MAPBOX_TOKEN}
        plugins={[keplerPlugin]}
      />
    </div>
  );
}

export default App;
