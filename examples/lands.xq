for $person in fn:doc("characters")/Characters/Character
 let $racesingle:=fn:data($person/@Race),
     $raceplural:=(
      let $pluralword:=fn:doc("plural")/Words/Word[@Single eq $racesingle]/(fn:data(@Plural))
      return (if (fn:exists($pluralword))
       then $pluralword
       else fn:concat($racesingle,"s")
      )
     ),
     $racearticle:=(
      let $firstletter:=fn:substring($racesingle,1,1)
      return (
       if (fn:contains("aeiou",$firstletter))
        then "an"
        else "a"
      )
     )
 (: return (fn:concat($racearticle, " ", $racesingle, " - ", $raceplural)) :)
 return (
  for $land in fn:doc("locations")/Locations/Location[@InhabitantRace eq $raceplural]/(fn:data(@Name))
  return (
   fn:concat(fn:data($person/@Name), " is ", $racearticle, " ", $racesingle, " and ", $land, " is the land of ", $raceplural, ".")
  )
 )
