I have solar panels but no battery. Is getting a battery worthwhile? 

Get 30-min electricity generation/import/export time series, calculate 30-min electricity-consumption values then construct hypothetical import/export series assuming a 5 kWh battery fully charged overnight for 7p/kWH.

Import and export data comes from Octopus Energy - see repo `get-octopus-energy-usage-data`. 

Generation data comes from Solis Cloud - see repo `get-solis-cloud-generation-data`. These are cumulative-total values, transformed into generation-over-30-minute-interval values. (Linear interpolation is used).

Calculate costs using actual and hypothetical import/export series; the difference will give potential savings.

Output is in the Power BI report.