#!/home/kantz/bin/zsh
#SBATCH --job-name=encode_dbpedia_images
#SBATCH --array=0-330%10
#SBATCH -c 4
#SBATCH --mem 8G
#SBATCH --gres=gpu:rtx 
#SBATCH -p allgroups
#SBATCH --output=logs/encode_dbpedia_%A_%a.out
#SBATCH --error=logs/encode_dbpedia_%A_%a.err
#SBATCH --time=0:10:00

# Array size is 330, one for each parquet file.

datafile_id=$SLURM_ARRAY_TASK_ID



cd ..
source ../.venv/bin/activate


python dbpedia/encode_dbpedia.py --out-dir ./data/dbpedia/encoded_thumbnails --dbpedia-dir ./data/dbpedia/index --datafile ${datafile_id} --batch-size 8