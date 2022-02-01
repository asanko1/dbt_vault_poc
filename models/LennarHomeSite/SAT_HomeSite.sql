Select PKID	,
CDMHOMESITEID	,
HOMESITEID	,
ADDRESSLINE1	,
CITY	,
STATE	,
ZIPCODE	,
LOTNUMBER	,
ISLOTEXCLUDED	,
COMMUNITYCODE	,
COMMUNITYNAME	,
DIVISIONCODE	,
DIVISIONNAME	,
REGIONCODE	,
REGIONNAME	,
COMMUNITYPHASENUMBER	,
PLANCODE	,
ISPLANEXCLUDED	,
ELEVATIONCODE	,
ARCHTYPECODE	,
TEMPLATENUMBER	,
HOMESTATUS	,
LOTSTATUSCODE	,
ISBROKERCOMISSION	,
ISHOMESITESOLD	,
ISHOMESITECLOSED	,
ISHOMESITECANCELLED	,
ISMODELHOMESITE	,
ISSHELLBUILDING	,
ISCREDITREPAIRSALES	,
ISCONTINGENTSALES	,
ISTRANSFER	,
ISSPECHOME	,
TRENCHDATE	,
ORIGINALDELIVERYDATE	,
ESTIMATEDDELIVERYDATE	,
ACTUALDELIVERYDATE	,
CANCELLATIONDATE	,
SALEDATE	
 from 
{{ source('Homesite', 'HOMESITEMASTER') }}