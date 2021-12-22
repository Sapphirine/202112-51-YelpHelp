# EECS 6893: Final Project, Fall 2021 - Yelp Help!

The project consists of three parts.
- Scrapers (Scrapes data from Yelp.com) 
- NER model (Model training and inference to get menu items `Pseudo-menus`)
- Web app (Can be used by consumers to look at the generated menus by entering city and zipcode)

The scraped data is used to generate .

    Pseudo-menu: This is not the typical menu that we see in restaurants where they might have their own obscure names for common food items. For example "Eggs can’t be cheese" this is a burger. So our Pseudo-menu will just have one menu item named "burger".


## Installation
To run the web server, open your terminal and clone the repository

```shell
git clone https://github.com/Sapphirine/202112-51-YelpHelp.git
```

Then, create a virtual environment

```shell
conda create --name myenv
```

Activate your environment and install required dependencies from requirements.txt

```shell
conda activate myenv
pip install -r requirements.txt
```

After installation, `cd` into `webapp/backend` then you can start the server and see the results from local host on port 7777

```shell
python yelp_help_api.py
```

## Authors
* Bhavin Dhedhi (bdd2115@columbia.edu)
* Karthik Datta Yerram (ky2482@columbia.edu)
* Vasanth Margabandhu (vm2656@columbia.edu)
