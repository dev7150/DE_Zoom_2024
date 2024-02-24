SELECT
   DeceasedDetails.Name.FamilyName AS Familyname,
   DeceasedDetails.Name.FirstGivenName AS Firstname,
   DeceasedDetails.Age AS Age,
   DeceasedDetails.DateOfBirth.ToDate.Date AS DateofBirth,
   DeceasedDetails.ResidentialAddress.Line1 AS AddressOfDeath,
  DeceasedDetails.ResidentialAddress.Line2 AS AddressOfDeath_Line2,
    DeceasedDetails.ResidentialAddress.Suburb AS Suburb,
   DeceasedDetails.ResidentialAddress.Postcode AS Postcode,
    DeceasedDetails.WhereDidDeathOccur AS PlaceOfDeath,
 SystemData.RegistrationDateRange.ToDate.Date AS DeathRegisteredDate,
 CONCAT(Year, '_', Number) AS Death_ID
 FROM
  vdi.victorian_death_index