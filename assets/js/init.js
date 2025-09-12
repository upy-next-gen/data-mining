// Initialize DataTables and Plotly charts
document.addEventListener('DOMContentLoaded', function () {
  // DataTables init (if library present)
  if (window.jQuery && jQuery().DataTable) {
    jQuery('.datatable').DataTable({
      pageLength: 25,
      deferRender: true,
      order: [],
      language: {
        url: 'https://cdn.datatables.net/plug-ins/1.13.6/i18n/es-ES.json'
      }
    });
  } else {
    console.warn('DataTables no disponible. Las tablas se mostrarán sin interacción.');
  }

  // Plotly charts from window.__CHARTS__ (array of {id, spec})
  if (window.Plotly && Array.isArray(window.__CHARTS__)) {
    window.__CHARTS__.forEach(function (c) {
      try {
        Plotly.newPlot(c.id, c.spec.data, c.spec.layout || {}, {responsive: true, displaylogo: false});
      } catch (e) {
        console.error('Error al renderizar gráfica', c.id, e);
      }
    });
  } else {
    console.warn('Plotly no disponible o no hay gráficas que renderizar.');
  }
});

