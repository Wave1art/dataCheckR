#This file contains all checks of two or more datasets.

#' CompareRows
#'
#' Function to do matched row based comparison between files without any preceding transformation to the data to be compared. This is in contrast to the function compareDimensions which first aggegates both tables over a common set of dimensions.
#'
#' @param table1
#' @param table2
#' @param commonKey A string or vector which specify the field(s) which should be used to join rows in between the two tables. The key must be unique in both tables to avoid problems.
#' @param testType A string or vector specifying the type of test to be performmed on each field. Currently supports "numeric"
#' @param testTolerance A string or vector specifying the tolerance that should be applied for each tested column
#'
#' @seealso compareDimensions
#' @export
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


#' compareDimensions
#'
#' CompareDimensions description
#'
#' @param table1 First table to compare
#' @param table2 Second table to compare
#' @param commonColumns A string or vector giving dimensions which should be used to compare both tables
#' @param columnsToTest A string or vector giving the columns which should be tested for each common set of dimensions.
#' @param columnAggregations A list of aggregations and corresponding labels to be performed on each in column given in columnsToTest as the dataset is rolled up
#' @param testTolerance A single value  or vector giving the tolerance to be used to assess whether a test 'passes' or not. If a single value is given this will be used for all columns to be tested.
#'
#' @seealso compareRows Does a row by row comparison with no pre-aggregation of the data
#'
#' @export
compareDimensions = function(table1, table2, commonColumns, columnsToTest, columnAggregations, tolerance){

  #Prepare dataset by aggregating each to a common level of detail
  aggregatedColumnNames = expand.grid(columnsToTest, names(columnAggregations)) %>% unite(., 'out', sep = '_') %>% pull(out)

  table1.agg = table1 %>% group_by_at(commonColumns, add = F) %>% summarise_at(columnsToTest, columnAggregations) %>% rename_at( aggregatedColumnNames, ~patste0( ., '_orig'))
  table2.agg = table2 %>% group_by_at(commonColumns, add = F) %>% summarise_at(columnsToTest, columnAggregations) %>% rename_at( aggregatedColumnNames, ~patste0( ., '_new'))

  #Basic checks of the datasets
  #ToDo - check that all unique elements in each dimension are represented in both dataets, that combinations of dimensions appear in both datasets. If not any lower level checks will most certainly fail.


  #more detailed checks
  summary.list = c()
  summary.passed = c()
  summary.failed = c()

  #Join the two datasets then compute comparisons and tests
  df.compare = table1.agg %>% full_join(table2.agg, by = commonColumns) %>% ungroup()

  for (i in aggregatedColumnNames){
    #construct variable names for the column being tested
    varname.diff = paste0(i, '_diff')
    varname.test = paste0(i, '_test')
    var.orig = paste0(i, '_orig')
    var.new = paste0(i, '_new')

    #do the comparisons and add the extra columns
    df.compare = df.compare %>%
      mutate(!! varname,diff := .data[[var.new]] - .data[[var.orig]]) %>%
      mutate(!! varname.test := abs(.data[[varname.diff]]) <= toleranvce)

    #add to summary objects which will be accessible after the test is run
    if( sum(df.compare %>% select(.data[[varname.test]]) %>% pul(1) == nrow(df.compare))){
      #all rows passed
      summary.list = c(summary.list, paste0(i, ': Pass'))
      summary.passed = c(summary.passed, i)
    }
    else{
      #one or more rows failed
      summary.list = c(summary.list, paste0(i, ': Fail'))
      summary.failed = c(summary.failed, i)
    }

    #return output objects
    return( list(
      summary = paste0('Comparison of to tables to ensure consistency across common DIMENSIONS',
                       '\nOverall status of tests: ', if(length(summary.failed >= 1 )){'Failed'} else{'Passed'},
                       '\n\nDimensions compared: ', commonColumns,
                       '\nFields tested: ', columnsToTest
                       ),
      summary.passed = summary.passed,
      summary.failed = summary.failed,
      df.compare = df.compare
    )
    )
  }


  }
