#This file contains all checks of two or more datasets where the tests are performed on matched rows.

#' Function to do matched row based comparison between files
#'
#' @param table1
#' @param table2
#' @param commonKey A string or vector which specify the field(s) which should be used to join rows in between the two tables. The key must be unique in both tables to avoid problems.
#' @param testType A string or vector specifying the type of test to be performmed on each field. Currently supports "numeric"
#' @param testTolerance A string or vector specifying the tolerance that should be applied for each tested column

compareRows = function(table1, table2, commonKey, testColumn, testType, testTolerance){

  #ToDo: check for duplicates in either of the two datasets and stop / warn the user


  #Construct the dataframe combining both datasets and
  compare = full_join(table1 %>% select(!!commonKey, df1 = !!testColumn),
                      table2 %>% select(!!commonKey, df2 = !!testColumn),
                      by = !!commonKey
                      )

  #Run the tests
  #ToDo: generalise this to more columns and add an overall row check when multiple columns are specified
  if(tolower(testType) == 'numeric'){ compare %<>% mutate(diff = df2 - df1, test = abs(diff) <= testTolerance) }
  else{
    #the test type is not recognised. Error
    stop('The value of test.type is not recognised.')
  }

  #compute the summaries and output
  overallTestStatus = sum(!compare$test) == 0
  overallRowsPassed = sum(compare$test)
  overallRowsFailed = sum(!compare$test)

  return(list(
    summary = paste0('Row by Row comparison of datasets.\n',
                     '\nJoining based on common set of fields: ', commonKey,
                     '\nTests were performed on the following fields: ', testColumn,
                     '\n\nOverall Status: ', if( overallTestStatus == 1 ){'Passed'} else { 'Failed' },
                     '\nRows passed: ', overallRowsPassed, '\nRows failed: ', overallRowsFailed)
  ))

}
