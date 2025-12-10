$(document).ready(function() {
    // File upload preview
    $('#file').on('change', function() {
        const fileName = $(this).val().split('\\').pop();
        if (fileName) {
            $(this).next('.form-text').html(`Selected: <strong>${fileName}</strong>`);
        }
    });

    // Preview data button
    $('#previewBtn').on('click', function() {
        const fileInput = $('#file')[0];
        if (!fileInput.files.length) {
            alert('Please select a file first');
            return;
        }

        const formData = new FormData();
        formData.append('file', fileInput.files[0]);

        const $btn = $(this);
        $btn.prop('disabled', true).html('<i class="fas fa-spinner fa-spin me-1"></i>Loading...');

        $.ajax({
            url: '/preview',
            type: 'POST',
            data: formData,
            processData: false,
            contentType: false,
            success: function(response) {
                if (response.error) {
                    alert('Error: ' + response.error);
                } else {
                    // Build preview table
                    let tableHtml = `
                        <div class="alert alert-info">
                            <strong>File Info:</strong> ${response.shape[0]} rows × ${response.shape[1]} columns
                        </div>
                        <table class="table table-sm table-bordered table-hover">
                            <thead class="table-light">
                                <tr>
                                    <th>#</th>
                                    ${response.columns.map((col, index) => 
                                        `<th title="Data type: ${response.dtypes[col] || 'unknown'}">${col}</th>`
                                    ).join('')}
                                </tr>
                            </thead>
                            <tbody>
                                ${response.head.map((row, rowIndex) => `
                                    <tr>
                                        <td class="text-muted">${rowIndex + 1}</td>
                                        ${response.columns.map(col => 
                                            `<td>${row[col] !== null && row[col] !== undefined ? row[col] : 
                                              '<span class="text-muted fst-italic">null</span>'}</td>`
                                        ).join('')}
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                        <div class="mt-2">
                            <small class="text-muted">
                                Missing values per column: 
                                ${Object.entries(response.missing_values).map(([col, count]) => 
                                    `${col}: ${count}`
                                ).join(', ')}
                            </small>
                        </div>
                    `;
                    
                    $('#previewContent').html(tableHtml);
                    $('#previewSection').removeClass('d-none');
                }
            },
            error: function(xhr) {
                alert('Error loading preview. Please try again.');
            },
            complete: function() {
                $btn.prop('disabled', false).html('<i class="fas fa-eye me-1"></i>Preview Data');
            }
        });
    });

    // Form submission with loading indicator
    $('#uploadForm').on('submit', function() {
        const $submitBtn = $(this).find('button[type="submit"]');
        $submitBtn.prop('disabled', true).html('<i class="fas fa-spinner fa-spin me-2"></i>Cleaning Data...');
        
        // Show progress indicator
        if (!$('#loadingIndicator').length) {
            $(this).append(`
                <div id="loadingIndicator" class="text-center my-3">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <p class="mt-2">Processing your data. This may take a moment...</p>
                </div>
            `);
        }
    });

    // Accordion toggle icons
    $('.accordion-button').on('click', function() {
        const icon = $(this).find('i');
        if ($(this).hasClass('collapsed')) {
            icon.removeClass('fa-chevron-down').addClass('fa-chevron-up');
        } else {
            icon.removeClass('fa-chevron-up').addClass('fa-chevron-down');
        }
    });

    // Initialize accordion icons
    $('.accordion-button').each(function() {
        if (!$(this).hasClass('collapsed')) {
            $(this).find('i').removeClass('fa-chevron-down').addClass('fa-chevron-up');
        }
    });

    // Range slider value display
    $('input[type="range"]').on('input', function() {
        const value = $(this).val();
        const percent = Math.round(value * 100);
        $(this).next().find('small:nth-child(2)').text(percent + '%');
    });
});

// Results page functionality
function downloadCleanedData() {
    // This function would be called from results.html
    window.location.href = $('#downloadBtn').data('url');
}

function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function() {
        alert('Copied to clipboard!');
    });
}