# USGS Streamgage Data Auto Retrieval Script

This script facilitates the automated downloading of USGS gauge data. It retrieves streamflow data from the USGS National Water Information System (NWIS) Water Services API and water-quality data from the current Water Quality Portal CSV endpoint.

## Author
Dongchen Wang

## Date
2024-10-15

## Version
1.1

## Usage
- Ensure you have an active internet connection.
- Customize the parameters as needed for your specific use case.
- Run the script in a Python environment.
- `downloadUSGS(siteNo, dtype, startDT, endDT, saveheaderparth=None, printHeader=True)` is for USGS streamgage time series data retrieval.
- `downloadUSGSWQ(siteNo, dtype, paramgroup=None, saveheaderparth=None, printHeader=True, characteristic_name=None)` is for USGS water-quality data retrieval.
  - `paramgroup` is kept for backward compatibility and maps to the Water Quality Portal `characteristicGroup` query.
  - `characteristic_name` is recommended when you want a narrower query such as `Suspended Sediment Concentration (SSC)`.
- `convertCommonUnitsToSI(df)` converts common streamflow and suspended-sediment units into SI-friendly columns.
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
- Water Quality Portal Result API: https://www.waterqualitydata.us/
