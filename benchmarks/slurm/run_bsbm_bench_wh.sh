#!/usr/bin/zsh
#SBATCH --job-name=run_bsbm_bench
#SBATCH --array=0-17%2
#SBATCH -c 4
#SBATCH --mem 4G

# #SBATCH -p allgroups
#SBATCH --output=logs/run_bsbm_bench_%A_%a.out
#SBATCH --error=logs/run_bsbm_bench_%A_%a.err
#SBATCH --time=0:20:00

# Array size is 5x3=15 
# -- 6 powers
# -- 3 db types
MAX_POWER=6
db_type_id=$((SLURM_ARRAY_TASK_ID / $MAX_POWER))
power_id=$((SLURM_ARRAY_TASK_ID % $MAX_POWER))
echo "db_type_id: ${db_type_id}, power_id: ${power_id}"
db_types=("qlever" "qlever-tidx" "fuseki")
db_type=${db_types[$((db_type_id+1))]}

cd ..
source ../.venv/bin/activate

echo "Running BSBM benchmark with db $db_type and power $power_id"

python run_bsbm_bench.py --db $db_type --power $power_id --id $SLURM_ARRAY_TASK_ID

echo "DONE"