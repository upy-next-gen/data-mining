// Initialize DataTables and Plotly charts
document.addEventListener('DOMContentLoaded', function () {
  // DataTables init (if library present)
  if (window.jQuery && jQuery().DataTable) {
    jQuery('.datatable').DataTable({
      pageLength: 25,
      deferRender: true,
      order: [],
      language: {
        decimal: ',',
        thousands: '.',
        emptyTable: 'No hay datos disponibles',
        info: 'Mostrando _START_ a _END_ de _TOTAL_ entradas',
        infoEmpty: 'Mostrando 0 a 0 de 0 entradas',
        infoFiltered: '(filtrado de _MAX_ entradas totales)',
        lengthMenu: 'Mostrar _MENU_ entradas',
        loadingRecords: 'Cargando...',
        processing: 'Procesando...',
        search: 'Buscar:',
        zeroRecords: 'No se encontraron resultados',
        paginate: { first: 'Primero', last: 'Último', next: 'Siguiente', previous: 'Anterior' },
        aria: { sortAscending: ': activar para ordenar ascendente', sortDescending: ': activar para ordenar descendente' }
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
