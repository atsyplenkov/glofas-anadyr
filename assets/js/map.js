const baseUrl = window.baseUrl || "";

async function fetchWithFallback(urls) {
  for (const url of urls) {
    try {
      const res = await fetch(url);
      if (res.ok) return res;
    } catch (_) {
      /* ignore and try next */
    }
  }
  throw new Error(`Failed to load gauges data from ${urls.join(", ")}`);
}

const map = new maplibregl.Map({
  container: 'map',
  style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  center: [170.5, 65.5],
  zoom: 5
});

map.addControl(new maplibregl.NavigationControl());

map.on('load', async () => {
  const response = await fetchWithFallback([
    `${baseUrl}/_data/gauges.json`,
    '/_data/gauges.json'
  ]);
  const geojson = await response.json();

  map.addSource('gauges', {
    type: 'geojson',
    data: geojson
  });

  map.addLayer({
    id: 'gauge-points',
    type: 'circle',
    source: 'gauges',
    paint: {
      'circle-radius': 8,
      'circle-color': '#2563eb',
      'circle-stroke-width': 2,
      'circle-stroke-color': '#ffffff'
    }
  });

  map.on('click', 'gauge-points', (e) => {
    const props = e.features[0].properties;
    const coords = e.features[0].geometry.coordinates.slice();

    const html = `
      <div class="popup-title">${props.name}</div>
      <div class="popup-river">${props.river} River</div>
      <div class="popup-stats">
        <strong>Observations:</strong> ${props.obs_start || '—'} to ${props.obs_end || '—'}<br>
        <strong>Missing:</strong> ${props.missing_pct}%<br>
        <strong>KGE' Raw:</strong> ${props.kge_raw || '—'}<br>
        <strong>KGE' DQM:</strong> ${props.kge_dqm || '—'}
      </div>
      <a class="popup-link" href="${baseUrl}/gauges/${props.id}/">View time series →</a>
    `;

    new maplibregl.Popup()
      .setLngLat(coords)
      .setHTML(html)
      .addTo(map);
  });

  map.on('mouseenter', 'gauge-points', () => {
    map.getCanvas().style.cursor = 'pointer';
  });

  map.on('mouseleave', 'gauge-points', () => {
    map.getCanvas().style.cursor = '';
  });
});
