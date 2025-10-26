[paths]
input_dir = folders/input
waiting_dir = folders/waiting
working_dir = folders/working
products_dir = folders/products
state_file = pipeline_state.json

[orca]
orca_path = /path/to/orca
method = B3LYP
basis = def2-SVP
solvent = none
solvent_model = CPCM
nprocs = 4
maxcore_mb = 2000
timeout_seconds = 3600
scf_convergence = TightSCF

[execution]
max_parallel_jobs = 5
max_retries = 2
check_interval_seconds = 5

[gmail]
enabled = false
sender_email = your_email@gmail.com
sender_password = your_app_specific_password
recipient_email = recipient@example.com

[notification]
notify_on_completion = true
notify_on_error = true
notification_threshold = 5
notification_interval_minutes = 60

[logging]
log_dir = logs
log_level = INFO
