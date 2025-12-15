let chartData = null;

async function loadChart() {
  const gaugeId = window.gaugeId;
  const baseUrl = window.baseUrl || "";
  const urls = [
    `${baseUrl}/data/timeseries/${gaugeId}.json`,
    `/data/timeseries/${gaugeId}.json`
  ];

  let response = null;
  for (const url of urls) {
    try {
      const res = await fetch(url);
      if (res.ok) {
        response = res;
        break;
      }
    } catch (_) {
      /* try next */
    }
  }

  if (!response) throw new Error('Failed to load timeseries data');
  const json = await response.json();
  chartData = json;

  const data = json.data.map(row => [
    new Date(row[0]),
    row[1],
    row[2],
    row[3]
  ]);

  new Dygraph(document.getElementById('chart'), data, {
    labels: ['Date', 'Observed', 'Raw GloFAS', 'Corrected'],
    title: window.gaugeId,
    ylabel: 'Daily water discharge, cms',
    showRangeSelector: true,
    colors: ['#1f77b4', '#ff7f0e', '#2ca02c'],
    strokeWidth: 1.5,
    connectSeparatedPoints: false,
    drawPoints: false,
    legend: 'always',
    labelsSeparateLines: true,
    highlightCircleSize: 4,
    highlightSeriesOpts: {
      strokeWidth: 2.5,
      highlightCircleSize: 5
    }
  });
}

function downloadCSV() {
  if (!chartData) return;

  const rows = [['date', 'q_cor']];
  chartData.data.forEach(row => {
    if (row[3] !== null) {
      rows.push([row[0], row[3]]);
    }
  });

  const csv = rows.map(r => r.join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);

  const a = document.createElement('a');
  a.href = url;
  a.download = `${window.gaugeId}_corrected.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

loadChart().catch(err => console.error('Chart load error:', err));
