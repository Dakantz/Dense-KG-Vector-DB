# Data Preparation

## BSDBM

This dataset is automatically set up once calling the `.setup()` method on the dataset class and can be encoded using `.encode()` methods.

## DBPedia

Reproducing this dataset may take some time - download and build the `.nt.bz2` file using

```sh
cd dbpedia
mkdir -p prepare && cd prepare
wget https://databus.dbpedia.org/dbpedia/generic/images/2022.12.01/images_lang=en.ttl.bz2
# formats the ugly data from dbpedia using sed
sh prepare_images.sh
#download the rest
sh download.sh
# combine into a single nt, error log as there are still a lot of broken URIs
riot --merge --output=NT --nocheck ./*.ttl.bz2 ./*.ttl.bzip2 ./*.nt   > ../dbpedia_complete.nt 2> riot_err.log
mkdir index
docker run --rm -v ./index:/data -v ./:/ttl -e UID=501 -e GID=220 -w /data qlever:tensors 'qlever-index -f /ttl/dbpedia_complete.nt -i dbpedia -m 1GB'
cd ..

```

Then encode the dataset (i.e. all image data) you can use
```sh
# assuming you are in benchmarks
python encode_dbpedia.py
```
This may take some time, you can parallelize the encoding on a slurm cluster using 
```
sbatch encode_dbpedia.sh
```
