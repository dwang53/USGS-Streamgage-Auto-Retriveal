# USGS Streamgage Data Auto Retrieval Script

This script facilitates the automated downloading of USGS gauge data. It retrieves streamflow data from the USGS National Water Information System (NWIS) Water Services API and water-quality data from the current Water Quality Portal CSV endpoint.

## Author
Dongchen Wang

## Date
2024-10-15

## Version
1.1

## Current workflow

This repository now supports two complementary USGS data paths:

- Streamflow and stage time series from the USGS Water Services API through `downloadUSGS(...)`
- Water-quality result tables from the Water Quality Portal through `downloadUSGSWQ(...)`

The intended pattern is:

1. Download raw streamgage time series with `downloadUSGS(...)`
2. Download raw water-quality tables with `downloadUSGSWQ(...)`
3. Save the raw outputs with `df.to_csv(...)`
4. Convert common discharge, stage, velocity, and suspended-sediment units to SI using `convertCommonUnitsToSI(df)`

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

## Supported unit conversions

`convertCommonUnitsToSI(df)` currently handles these common fields:

- `ft3/s` to `m3/s`
- `ft` to `m`
- `ft/s` to `m/s`
- `mg/L` to `kg/m3`
- `%` to fraction
- `tons/day` to `kg/s` using short tons

## Notes

- The old NWIS `qwdata` path used in earlier versions is no longer reliable for current water-quality downloads.
- The updated water-quality workflow uses the Water Quality Portal result API instead.
- For suspended sediment retrieval, a targeted `characteristic_name` query such as `Suspended Sediment Concentration (SSC)` is recommended instead of broad legacy group queries.

## Dependencies
- `urllib.request`
- `urllib.parse`
- `pandas`
- `numpy`

## Example
- Run the script: `python usgs_streamgage_retrieval.py`
- Or check the Jupyter notebook example: `USGS_Streamgage-Auto-Retriveal_Example.ipynb`
![ExamplePlot](https://github.com/user-attachments/assets/c34fc0bf-fe25-4215-be29-8e8125378de9)


## References
- USGS Water Services API Documentation: https://waterservices.usgs.gov/
- Water Quality Portal Result API: https://www.waterqualitydata.us/

## Update log

- `2026-04-07`
  - updated water-quality retrieval to use the current Water Quality Portal result API
  - kept `downloadUSGSWQ(...)` backward-compatible while adding `characteristic_name=...`
  - added `convertCommonUnitsToSI(df)` for common streamflow and suspended-sediment SI conversion
  - updated the README to reflect the current workflow and supported conversions
