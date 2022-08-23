rm(list=ls())
library(tidyverse)
library(orderstats)
library(Pareto)
library(parallel)
library(doParallel)
library(readr)
library(EnvStats)
library(dplyr)
ncores <- detectCores() -2

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/WAR/raw_data")

batters_fWAR <- read.csv('batters_fWAR.csv')
batters_bWAR <- read.csv('batters_bWAR.csv')%>% filter(!WAR %in% 'NULL')
batters_bWAR[is.na(batters_bWAR$lg_ID),]$lg_ID <- 'NL'
batters_bWAR$PA <- as.numeric(as.character(batters_bWAR$PA))
batters_bWAR$WAR <- as.numeric(as.character(batters_bWAR$WAR))
batters_bWAR$age <- as.numeric(as.character(batters_bWAR$age))

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/eligible_pop")

MLB_pop <- read.csv('datMLBpop.csv') 

## add weighted populations to data
pops_data <- rbind(
  data.frame(pops = MLB_pop$NL_pop, lgID = "NL", yearID = 1870 + 1:(15*10 + 1)),
  data.frame(pops = MLB_pop$AL_pop, lgID = "AL", yearID = 1870 + 1:(15*10 + 1)))

pop_batters <- merge(batters_fWAR, pops_data, by = c("yearID", "lgID"))

batters <- do.call(rbind, mclapply(split(pop_batters, f = droplevels(as.factor(pop_batters$playerid))), mc.cores = ncores, FUN = function(xx){
  n <- unique(xx$yearID)
  G <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(G)))
  lgID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(lgID))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(WAR)))])
  teamID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(Team))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(WAR)))])
  PA <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(PA)))
  AB <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(AB)))
  WAR <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(WAR)))
  pops <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(pops))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(WAR)))])
  tibble(playerID = unique(xx$playerid), name = unique(xx$Name), 
         yearID = n,G = G, lgID = lgID, teamID = teamID, AB = AB, PA = PA, WAR = WAR, pops = pops )
} 
)) %>% mutate(WAR_G = WAR / G)
setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/WAR/raw_data")
write.csv(batters, 'batters_combined_f.csv')

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/eligible_pop")

MLB_pop <- read.csv('datMLBpop.csv') 

## add weighted populations to data
pops_data <- rbind(
  data.frame(pops = MLB_pop$NL_pop, lg_ID = "NL", year_ID = 1870 + 1:(15*10 + 1)),
  data.frame(pops = MLB_pop$AL_pop, lg_ID = "AL", year_ID = 1870 + 1:(15*10 + 1)))

pop_batters <- merge(batters_bWAR, pops_data, by = c("year_ID", "lg_ID"))

batters <- do.call(rbind, mclapply(split(pop_batters, f = droplevels(as.factor(pop_batters$player_ID)))[1:10000], mc.cores = ncores, FUN = function(xx){
  n <- unique(xx$year_ID)
  G <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(G)))
  age <- sapply(n, function(yy) unlist(xx %>% filter(year_ID == yy) %>% select(age))[1])
  lgID <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(lg_ID))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  teamID <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(team_ID))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  PA <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(PA)))
  WAR <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(WAR)))
  pops <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(pops))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  tibble(playerID = unique(xx$player_ID), name = unique(xx$name_common), 
             yearID = n,G = G, age = age, lgID = lgID, teamID = teamID, PA = PA, WAR = WAR, pops = pops)
  }
)) 

batters_2nd <- do.call(rbind, mclapply(split(pop_batters, f = droplevels(as.factor(pop_batters$player_ID)))[10001:18605], mc.cores = ncores, FUN = function(xx){
  n <- unique(xx$year_ID)
  G <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(G)))
  age <- sapply(n, function(yy) unlist(xx %>% filter(year_ID == yy) %>% select(age))[1])
  lgID <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(lg_ID))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  teamID <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(team_ID))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  PA <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(PA)))
  WAR <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(WAR)))
  pops <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(pops))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  tibble(playerID = unique(xx$player_ID), name = unique(xx$name_common), 
         yearID = n,G = G, age = age, lgID = lgID, teamID = teamID, PA = PA, WAR = WAR, pops = pops)
}
)) 

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/WAR/raw_data")
write.csv(rbind(batters, batters_2nd), 'batters_combined_b.csv')








