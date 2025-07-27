import React from 'react';
import KeplerGl from 'kepler.gl';

export default function MapPanel({ highlighted }) {
  return (
    <div className="w-full h-full">
      <KeplerGl
        id="map"
        width={window.innerWidth}
        height={window.innerHeight}
        mapboxApiAccessToken={process.env.REACT_APP_MAPBOX_TOKEN}
      />
    </div>
  );
}
