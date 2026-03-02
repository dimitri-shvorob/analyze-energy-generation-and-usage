I have solar panels but no battery. Is getting a battery worthwhile? 

Get 30-min electricity generation/import/export time series, calculate 30-min consumption values, then construct hypothetical import/export series assuming a 5 kWh battery fully charged overnight to for 7 p/kWH.

(Import and export data come from Octopus Energy - see repo `get-octopus-energy-usage-data`. 

Generation data come from Solis Cloud - see repo `get-solis-cloud-generation-data`. These are 5-min cumulative-value snapshots, crudely converted to 30-min totals). 

Calculate costs using actual and hypothetical import/export series; the difference will give potential savings. (Ignoring the battery-installation costs, of course).

Output is in the Power BI report.

Notes:
- The cumulative-to-periodic conversion applied to Solis Cloud data could be improved. (But the current numbers should be "close enough").
- Working with 30-min data is slightly problematic on Octopus Energy side as well - for example, you get negative consumption values. (But, again, the distortion should not be major). 
- Octopus Energy export data goes wonky (does not match monthly bill amounts, falls short by 25-50%!) beginning in October 2025. (But cold-season solar yields are tiny anyway).   