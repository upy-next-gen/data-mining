
    document.addEventListener('DOMContentLoaded', function () {
        // --- Chart Logic ---
        const selector = document.getElementById('period-selector');
        const ctx = document.getElementById('rankingChart').getContext('2d');
        let rankingChart;

        const periods = Object.keys(dashboardData);
        periods.forEach(p => {
            const option = document.createElement('option');
            option.value = p;
            option.textContent = p;
            selector.appendChild(option);
        });

        function updateChart(period) {
            const chartData = dashboardData[period];
            if (rankingChart) {
                rankingChart.destroy();
            }
            rankingChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: chartData.labels,
                    datasets: [{ 
                        label: '% de Percepción de Inseguridad',
                        data: chartData.data,
                        backgroundColor: 'rgba(52, 152, 219, 0.7)',
                        borderColor: 'rgba(41, 128, 185, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    indexAxis: 'y',
                    scales: { x: { beginAtZero: true, max: 100 } },
                    plugins: { legend: { display: false } }
                }
            });
        }

        selector.addEventListener('change', (event) => {
            updateChart(event.target.value);
        });

        selector.value = '2025-T2';
        updateChart('2025-T2');

        // --- Table Filtering Logic ---
        const yearFilter = document.getElementById('year-filter');
        const tableRows = document.querySelectorAll('#data-table tbody tr');

        yearFilter.addEventListener('change', (event) => {
            const selectedYear = event.target.value;
            tableRows.forEach(row => {
                const rowYear = row.getAttribute('data-year');
                if (selectedYear === 'all' || rowYear === selectedYear) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            });
        });
    });
    