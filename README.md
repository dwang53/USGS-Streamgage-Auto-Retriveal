# USGS Streamgage Data Auto Retrieval Script

This script facilitates the automated downloading of USGS gauge data. It retrieves real-time streamflow data from the USGS National Water Information System (NWIS) using the USGS Water Services API.

## Author
Dongchen Wang

## Date
2024-10-15

## Version
1.0

## Usage
- Ensure you have an active internet connection.
- Customize the parameters as needed for your specific use case.
- Run the script in a Python environment.
- `downloadUSGS(siteNo, dtype, startDT, endDT, saveheaderparth=None, printHeader=True)` is for USGS streamgage time series data retrieval.
- `downloadUSGSWQ(siteNo, dtype, paramgroup, saveheaderparth=None, printHeader=True)` is for USGS water quality data retrieval.
- Use `df.to_csv(fname)` to save the data file.
- Use `readDownloadedData(fname)` to read the downloaded data file.

## Dependencies
- `urllib.request`
- `pandas`
- `numpy`

## Example
- Run the script: `python usgs_streamgage_retrieval.py`
- Or check the Jupyter notebook example: `USGS_Streamgage-Auto-Retriveal_Example.ipynb`
![ExamplePlot](https://github.com/user-attachments/assets/c34fc0bf-fe25-4215-be29-8e8125378de9)


## References
- USGS Water Services API Documentation: https://waterservices.usgs.gov/
