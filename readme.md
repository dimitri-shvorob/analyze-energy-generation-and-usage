I have solar panels but no battery. Is getting a battery worthwhile? 

Get 30-min electricity generation/import/export time series, calculate 30-min consumption values, then construct hypothetical import/export series assuming a 5 kWh battery charged overnight for 7 p/kWH.

(Import and export data come from Octopus Energy - see repo `get-octopus-energy-usage-data`. 

Generation data come from Solis Cloud - see repo `get-solis-cloud-generation-data`. These are 50-min cumulative-value series, (crudely) converted to 30-min totals). 

Calculate costs using actual and hypothetical import/export series; the difference will give potential savings. (Ignoring the battery-installation costs, of course).

Output is in the Power BI report.