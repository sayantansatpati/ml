#libraries needed
require(dplyr)
require(rpart)
require(ggplot2)
require(randomForest)

data = read.csv('conversion_data.csv')
head(data)
str(data)
summary(data)
hist(data$age)
