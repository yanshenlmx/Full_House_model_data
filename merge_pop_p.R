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

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers/WAR/raw_data")

pitchers_fWAR <- read.csv('pitchers_fWAR.csv')
pitchers_bWAR <- read.csv('pitchers_bWAR.csv')%>% filter(!WAR %in% 'NULL')
pitchers_bWAR[is.na(pitchers_bWAR$lg_ID),]$lg_ID <- 'NL'
pitchers_bWAR$lg_ID[pitchers_bWAR$lg_ID == 'AA'] <- 'NL'
pitchers_bWAR$lg_ID[pitchers_bWAR$lg_ID == 'UA'] <- 'NL'
pitchers_bWAR$lg_ID[pitchers_bWAR$lg_ID == 'PL'] <- 'NL'
pitchers_bWAR$lg_ID[pitchers_bWAR$lg_ID == 'FL'] <- 'NL'

pitchers_bWAR$IPouts <- as.numeric(as.character(pitchers_bWAR$IPouts))
pitchers_bWAR$WAR <- as.numeric(as.character(pitchers_bWAR$WAR))
pitchers_bWAR$age <- as.numeric(as.character(pitchers_bWAR$age))

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/eligible_pop")

MLB_pop <- read.csv('datMLBpop.csv') 

## add weighted populations to data
pops_data <- rbind(
  data.frame(pops = MLB_pop$NL_pop, lgID = "NL", yearID = 1870 + 1:(15*10 + 1)),
  data.frame(pops = MLB_pop$AL_pop, lgID = "AL", yearID = 1870 + 1:(15*10 + 1)))

pop_pitchers <- merge(pitchers_fWAR, pops_data, by = c("yearID", "lgID"))

pitchers <- do.call(rbind, mclapply(split(pop_pitchers, f = droplevels(as.factor(pop_pitchers$playerid))), mc.cores = ncores, FUN = function(xx){
  n <- unique(xx$yearID)
  lgID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(lgID))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(WAR)))])
  teamID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(Team))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(WAR)))])
  G <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(G)))
  IP <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(IP)))
  WAR <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(WAR)))
  pops <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(pops))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(WAR)))])
  tibble(playerID = unique(xx$playerid), name = unique(xx$Name),
         yearID = n,lgID = lgID, teamID = teamID, G= G, IP = IP, WAR = WAR, pops = pops)
}
))

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers/WAR/raw_data")
write.csv(pitchers, 'pitchers_combined_f.csv')

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/eligible_pop")

MLB_pop <- read.csv('datMLBpop.csv') 

## add weighted populations to data
pops_data <- rbind(
  data.frame(pops = MLB_pop$NL_pop, lg_ID = "NL", year_ID = 1870 + 1:(15*10 + 1)),
  data.frame(pops = MLB_pop$AL_pop, lg_ID = "AL", year_ID = 1870 + 1:(15*10 + 1)))

pop_pitchers <- merge(pitchers_bWAR, pops_data, by = c("year_ID", "lg_ID"))

pitchers <- pop_pitchers %>% group_by(player_ID, year_ID) %>% 
  summarise(playerID = unique(player_ID), yearID = unique(year_ID), 
            name = unique(name_common), G = sum(G), age = age[1],
            lgID = lg_ID[which.max(WAR)], 
            teamID = team_ID[which.max(WAR)], IPouts = sum(IPouts), 
            WAR = sum(WAR), pops = mean(pops))


pitchers <- do.call(rbind, mclapply(split(pop_pitchers, f = droplevels(as.factor(pop_pitchers$player_ID))), mc.cores = ncores, FUN = function(xx){
  n <- unique(xx$year_ID)
  G <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(G)))
  age <- sapply(n, function(yy) unlist(xx %>% filter(year_ID == yy) %>% select(age))[1])
  lgID <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(lg_ID))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  teamID <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(team_ID))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  IPouts <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(IPouts)))
  WAR <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(WAR)))
  pops <- sapply(n, function(yy) as.matrix(xx %>% filter(year_ID == yy) %>% select(pops))[which.max(as.matrix(xx %>% filter(year_ID == yy) %>% select(WAR)))])
  tibble(playerID = unique(xx$player_ID), name = unique(xx$name_common), 
             yearID = n,G = G, age = age, lgID = lgID, teamID = teamID, IPouts = IPouts, WAR = WAR, pops = pops)
  }
))

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers/WAR/raw_data")
write.csv(pitchers, 'pitchers_combined_b.csv')








