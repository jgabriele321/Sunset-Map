document.addEventListener('DOMContentLoaded', function() {
    const processButton = document.getElementById('processButton');
    const processingStatus = document.getElementById('processingStatus');
    const resultsSection = document.getElementById('resultsSection');
    let map = null;
    let hourDistChart = null;

    // Initialize map
    function initMap() {
        if (!map) {
            map = L.map('map').setView([39.8283, -98.5795], 4);
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '© OpenStreetMap contributors'
            }).addTo(map);
        }
    }

    // Initialize charts
    function initCharts() {
        const ctx = document.getElementById('hourDistChart').getContext('2d');
        hourDistChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [{
                    label: 'Number of Locations',
                    data: [],
                    backgroundColor: 'rgba(13, 110, 253, 0.5)',
                    borderColor: 'rgba(13, 110, 253, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }

    // Format time string
    function formatTime(timeStr) {
        return timeStr;  // Already in correct format from API
    }

    // Update summary statistics
    function updateSummaryStats(data) {
        const stats = data.sunset_statistics.summary_statistics;
        document.getElementById('summaryStats').innerHTML = `
            <div class="row">
                <div class="col-6 mb-3">
                    <div class="stat-label">Average Sunset</div>
                    <div class="stat-value">${formatTime(stats.average_sunset)}</div>
                </div>
                <div class="col-6 mb-3">
                    <div class="stat-label">Median Sunset</div>
                    <div class="stat-value">${formatTime(stats.median_sunset)}</div>
                </div>
                <div class="col-6">
                    <div class="stat-label">Standard Deviation</div>
                    <div class="stat-value">${stats.standard_deviation_minutes} min</div>
                </div>
                <div class="col-6">
                    <div class="stat-label">Total Locations</div>
                    <div class="stat-value">${stats.total_locations}</div>
                </div>
            </div>
        `;
    }

    // Update range analysis
    function updateRangeAnalysis(data) {
        const range = data.sunset_statistics.range_analysis;
        document.getElementById('rangeAnalysis').innerHTML = `
            <div class="mb-3">
                <div class="stat-label">Earliest Sunset</div>
                <div class="stat-value">${formatTime(range.earliest_sunset.time)}</div>
                <small class="text-muted">ZIP: ${range.earliest_sunset.zip_code}</small>
            </div>
            <div class="mb-3">
                <div class="stat-label">Latest Sunset</div>
                <div class="stat-value">${formatTime(range.latest_sunset.time)}</div>
                <small class="text-muted">ZIP: ${range.latest_sunset.zip_code}</small>
            </div>
            <div>
                <div class="stat-label">Time Range</div>
                <div class="stat-value">${range.time_range_minutes} minutes</div>
            </div>
        `;
    }

    // Update hour distribution chart
    function updateHourDistribution(data) {
        const hours = data.sunset_statistics.hour_distribution;
        const labels = Object.keys(hours).sort();
        const values = labels.map(hour => {
            return {
                count: hours[hour].count,
                percentage: parseFloat(hours[hour].percentage)
            };
        });

        hourDistChart.data.labels = labels.map(h => `${h}:00`);
        hourDistChart.data.datasets[0].data = values.map(v => v.count);
        hourDistChart.update();
    }

    // Update timezone analysis
    function updateTimezoneAnalysis(data) {
        const timezones = data.sunset_statistics.timezone_analysis;
        const html = Object.entries(timezones)
            .sort(([a], [b]) => parseFloat(a) - parseFloat(b))
            .map(([offset, data]) => `
                <div class="timezone-item">
                    <div class="row">
                        <div class="col-6">
                            <strong>UTC${offset >= 0 ? '+' : ''}${offset}</strong>
                        </div>
                        <div class="col-6 text-end">
                            ${data.count} locations
                        </div>
                    </div>
                    <div class="row mt-1">
                        <div class="col-12">
                            <small class="text-muted">
                                Avg: ${formatTime(data.average)} 
                                (±${data.std_dev_minutes} min)
                            </small>
                        </div>
                    </div>
                </div>
            `).join('');

        document.getElementById('timezoneAnalysis').innerHTML = html;
    }

    // Start processing
    async function startProcessing() {
        try {
            processButton.disabled = true;
            processingStatus.classList.remove('d-none');

            const response = await fetch('/api/process', {
                method: 'POST'
            });

            if (!response.ok) {
                throw new Error('Processing failed');
            }

            const data = await response.json();
            displayResults(data.data);
        } catch (error) {
            showError(error.message);
        } finally {
            processButton.disabled = false;
            processingStatus.classList.add('d-none');
        }
    }

    // Display results
    function displayResults(data) {
        resultsSection.classList.remove('d-none');
        initMap();
        initCharts();
        
        updateSummaryStats(data);
        updateRangeAnalysis(data);
        updateHourDistribution(data);
        updateTimezoneAnalysis(data);
    }

    // Show error message
    function showError(message) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'error-message';
        errorDiv.textContent = `Error: ${message}`;
        processingStatus.parentNode.insertBefore(errorDiv, processingStatus.nextSibling);
        setTimeout(() => errorDiv.remove(), 5000);
    }

    // Check for existing results
    async function checkExistingResults() {
        try {
            const response = await fetch('/api/results');
            if (response.ok) {
                const data = await response.json();
                if (data.status === 'success') {
                    displayResults(data.data);
                }
            }
        } catch (error) {
            console.error('Error checking existing results:', error);
        }
    }

    // Event listeners
    processButton.addEventListener('click', startProcessing);

    // Check for existing results on page load
    checkExistingResults();
}); 