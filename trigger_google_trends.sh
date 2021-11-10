export PATH="/home/ubuntu/miniconda3/bin:$PATH"

cd /home/ubuntu/french-presidential-election-2022-data-collecter

conda activate

python google_trends_collecter.py
python google_trends_collecter.py --area="world"