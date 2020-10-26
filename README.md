# Getting Stock Pricing Data from Polygon.io's API using Python

Pulling and creating clean stock pricing data is the first step in developing and modeling any trading algorithms.  This code will pull stock pricing history in the format of daily bars including OHLCV (Open, High, Low, Close, Volume) data.  The bars will be adjusted for stock splits using the stock split data and a specifically developed function.  Dividend data will also be appended to the dataset for a complete view of returns.

You can sign up for an API key at [Polygon.io](https://polygon.io/).

Alternatively, you can use your [Alpaca](https://alpaca.markets/) Trading API key to get free access to Polygon data.  Note, in my code I save my API Key as an environmental variable (so on my MacOS I included it in my .bach_profile file as export ALPACA_API_KEY="YOURKEYHERE")

## Requirements

Download and install [Anaconda](https://www.anaconda.com/products/individual)

Create a new virtual environment

`$ conda create --name polyenv python=3.8`

Activate the new virtual environment

`$ conda activate polyenv`

Install packages

`$ pip install -r requirements.txt`

Alternatively, you can take a look at the requirements.txt file and install what you need.

Note, I'm using VS Code with the ipykernel (Jupyter Notebook) capability to run in a notebook like experience.  You can certainly also run this without this.


