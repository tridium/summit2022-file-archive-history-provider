<!---
   @author     Scott Hoye
   @creation   25 March 2022
   @since      Niagara Summit 2022
--->

# File Archive History Provider Example

Requiring a Niagara 4.11 (or later) environment, this module provides an example Archive History Provider 
implementation that retrieves archive history data from files, such as CSV files. In order to know how to 
parse archived history records from a particular file, this example provider will look for matching 
(enabled) Delimited (or Csv) File Import descriptors in the local station, which should reside somewhere 
under a File Network. When using this provider in a station, it is assumed that the station is licensed 
with Tridium license features `fileDriver` and `historyArchive`. The File Network must also be installed 
in the station to import file data to model as local histories. Those local histories can be configured 
with a limited (small) rolling capacity, leaving older (archived) history data in the source file. The 
archived file data will then be accessed on-demand via the File Archive History Provider to supplement 
local history data when history queries occur at runtime.

**PLEASE NOTE THAT THIS MODULE IS INTENDED TO BE AN EXAMPLE IMPLEMENTATION ONLY! IT IS NOT MEANT FOR PRODUCTION 
USE SINCE IT IS NOT OPTIMIZED IN TERMS OF MEMORY AND PERFORMANCE, AND IT ALSO RELIES ON SOME NON-PUBLIC 
NIAGARA APIs.**