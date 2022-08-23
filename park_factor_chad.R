rm(list=ls())
library(tidyverse)
library(orderstats)
library(Pareto)
library(parallel)
library(doParallel)
library(readr)
library(EnvStats)
library(dplyr)
ncores <- detectCores() -1

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/AVG/raw_data")
## raw data from Chadwick.
raw_batter <- read.csv("raw_batter.csv")
raw_batter[is.na(raw_batter$lgID),]$lgID <- 'NL'
batters <- raw_batter %>% filter(lgID %in% c('NL', 'AL')) %>% select(-c(stint,X2B, X3B, RBI:CS, SO, IBB, GIDP))

## park factor for Runs, Hits and Home runs.
years <- c(1901:2020)
team_log <- do.call(rbind, lapply(years, function(x) 
  cbind(read_csv(paste("teams-", x, ".csv", sep = "")), yearID = x)))

team_log <- team_log %>% select(c(team.alignment, team.key, opponent.key, 
                                  B_R, B_H, B_HR, P_R, P_H, P_HR, yearID))

park_factor_year <- function(year){
  team_log_year <- team_log %>% filter(yearID == year)
  teamID <- unique(team_log_year$team.key)
  teamID <- teamID[!teamID %in% c('ALS', 'NLS', 'NL1', 'NL2', 'AL1', 'AL2')]
  do.call(rbind, mclapply(teamID, mc.cores = ncores, FUN = function(xx){
    home_RS <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 1) %>%
                     select(B_R))
    home_HS <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 1) %>%
                     select(B_H))
    home_HRS <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 1) %>%
                     select(B_HR))
    
    home_RA <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 1) %>%
                     select(P_R))
    home_HA <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 1) %>%
                     select(P_H))
    home_HRA <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 1) %>%
                      select(P_HR))
    
    away_RS <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 0) %>%
                     select(B_R))
    away_HS <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 0) %>%
                     select(B_H))
    away_HRS <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 0) %>%
                      select(B_HR))
    
    away_RA <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 0) %>%
                     select(P_R))
    away_HA <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 0) %>%
                     select(P_H))
    away_HRA <- sum(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 0) %>%
                      select(P_HR))
    
    homeG <- nrow(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 1))
    awayG <- nrow(team_log_year %>% filter(team.key == xx) %>% filter(team.alignment == 0))
    
    n <- nrow(unique(team_log_year %>% select(team.key)))
    
    park_index_R <- ((home_RS+home_RA)/(homeG)) / ((away_RS+away_RA)/(awayG))
    park_index_H <- ((home_HS+home_HA)/(homeG)) / ((away_HS+away_HA)/(awayG))
    park_index_HR <- ((home_HRS+home_HRA)/(homeG)) / ((away_HRS+away_HRA)/(awayG))
    
    park_factor_R <- ((park_index_R +1)/2)/((park_index_R+n-1)/n)
    park_factor_H <- ((park_index_H +1)/2)/((park_index_H+n-1)/n)
    park_factor_HR <- ((park_index_HR +1)/2)/((park_index_HR+n-1)/n)
    
    data.frame(TeamID = xx, yearID = year, park_factor_R = park_factor_R, 
               park_factor_H = park_factor_H, park_factor_HR = park_factor_HR)
  }))
}

park_factor <- do.call(rbind, mclapply(years, mc.cores = ncores, FUN = function(xx){
  park_factor_year(xx)
}))
colnames(park_factor)[1] <- 'teamID'
m <- park_factor
m$teamID <- as.character(m$teamID)
m$teamID[m$yearID >= 2005] <- gsub('ANA', 'LAA', m$teamID[m$yearID >= 2005])
m$teamID[m$yearID >= 1953 & m$yearID <= 1965] <- gsub('MLN', 'ML1', m$teamID[m$yearID >= 1953 & m$yearID <= 1965])
m$teamID[m$yearID >= 1970 & m$yearID <= 1997] <- gsub('MIL', 'ML4', m$teamID[m$yearID >= 1970 & m$yearID <= 1997])
park_factor <- m

## add 2021 season park factor
## https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?type=year&year=2021&batSide=&stat=index_wOBA&condition=All&rolling=no
## make teamID consistent with the teamID in raw_batters
## CHA - CHW
## CHN - CHC
## KCA - KCR
## LAN - LAD
## NYA - NYY
## NYN - NYM
## SDN - SDP
## SFN - SFG
## SLN - STL
## TBA - TBR
## WAS - WSN
park_factor_2021 <- read.csv('park_factor_2021.csv')
n <- 30
park_factor_2021 <- park_factor_2021 %>% 
  mutate(park_factor_R = ((park_factor_R/100 +1)/2)/((park_factor_R/100+n-1)/n)) %>%
  mutate(park_factor_H = ((park_factor_H/100 +1)/2)/((park_factor_H/100+n-1)/n)) %>% 
  mutate(park_factor_HR = ((park_factor_HR/100 +1)/2)/((park_factor_HR/100+n-1)/n))

park_factor <- rbind(park_factor, park_factor_2021)

## add 2021 season batters
## CHA - CHW
## CHN - CHC
## KCA - KCR
## LAN - LAD
## NYA - NYY
## NYN - NYM
## SDN - SDP
## SFN - SFG
## SLN - STL
## TBA - TBR
## WAS - WSN
batters_2021 <- read.csv('batters_2021.csv',stringsAsFactors = FALSE)
batters_2021$teamID <- gsub('CHW', 'CHA', batters_2021$teamID)
batters_2021$teamID <- gsub('CHC', 'CHN', batters_2021$teamID)
batters_2021$teamID <- gsub('KCR', 'KCA', batters_2021$teamID)
batters_2021$teamID <- gsub('LAD', 'LAN', batters_2021$teamID)
batters_2021$teamID <- gsub('NYY', 'NYA', batters_2021$teamID)
batters_2021$teamID <- gsub('NYM', 'NYN', batters_2021$teamID)
batters_2021$teamID <- gsub('SDP', 'SDN', batters_2021$teamID)
batters_2021$teamID <- gsub('SFG', 'SFN', batters_2021$teamID)
batters_2021$teamID <- gsub('STL', 'SLN', batters_2021$teamID)
batters_2021$teamID <- gsub('TBR', 'TBA', batters_2021$teamID)
batters_2021$teamID <- gsub('WSN', 'WAS', batters_2021$teamID)

colnames(batters_2021)[6] <- 'lgID'

batters_2021$playerID <- replace(batters_2021$playerID, 
                                       batters_2021$playerID == 'alexasc02', 
                                       'alexasc01' )
batters_2021$playerID <- replace(batters_2021$playerID, 
                                       batters_2021$playerID == 'davieza02', 
                                       'davieza01' )
batters_2021$playerID <- replace(batters_2021$playerID, 
                                       batters_2021$playerID == 'harriwi10', 
                                       'harriwi02' )
batters_2021$playerID <- replace(batters_2021$playerID, 
                                       batters_2021$playerID == 'lintz02', 
                                       'lintz01' )
batters_2021$playerID <- replace(batters_2021$playerID, 
                                       batters_2021$playerID == 'montafr02', 
                                       'montafr01' )
batters_2021$playerID <- replace(batters_2021$playerID, 
                                       batters_2021$playerID == 'willima10', 
                                       'willima07' )
batters_2021$playerID <- replace(batters_2021$playerID, 
                                 batters_2021$playerID == 'rodrijo06', 
                                 'rodrijo04' )

batters <- rbind(batters, batters_2021 %>% 
                   mutate(yearID = 2021) %>% 
                   select('playerID', 'yearID', 'teamID', 'lgID', 
                          'G', 'AB', 'R', 'H', 'HR', 'BB', 'HBP', 
                          'SH', 'SF'))
## moving window for park factor. 
## check the consecutiveness. 

table(park_factor$teamID)

a <- do.call(rbind, mclapply(unique(park_factor$teamID), mc.cores = ncores, FUN = function(xx){
  m <- park_factor %>% filter(teamID == xx)
  if (max(m$yearID) - min(m$yearID) > nrow(m)) {
    m
  }
}))

moving_window_mean <- function(x){
  n <- length(x)
  y <- rep(0,n)
  if (n == 1) {
    y[1] = x[1]
  }
  if (n == 2) {
    y[1:2] = mean(x)
  }
  if (n >= 3) {
    y[1] <- (x[1] + x[2])/2
    y[2:(n-1)] <- (x[1:(n-2)] + x[2:(n-1)] + x[3:n])/3
    y[n] <- (x[n-1] + x[n])/2
  }
  y
}
park_factor_averaged <- do.call(rbind, mclapply(unique(park_factor$teamID), mc.cores = ncores, FUN = function(xx){
  if (xx == "LAA") {
    m <- park_factor %>% filter(teamID == xx)
    m$park_factor_R[1:4] <- moving_window_mean(m$park_factor_R[1:4])
    m$park_factor_H[1:4] <- moving_window_mean(m$park_factor_H[1:4])
    m$park_factor_HR[1:4] <- moving_window_mean(m$park_factor_HR[1:4])
    m$park_factor_R[5:21] <- moving_window_mean(m$park_factor_R[5:21])
    m$park_factor_H[5:21] <- moving_window_mean(m$park_factor_H[5:21])
    m$park_factor_HR[5:21] <- moving_window_mean(m$park_factor_HR[5:21])
    m
  } else{
    m <- park_factor %>% filter(teamID == xx)
    m$park_factor_R <- moving_window_mean(m$park_factor_R)
    m$park_factor_H <- moving_window_mean(m$park_factor_H)
    m$park_factor_HR <- moving_window_mean(m$park_factor_HR)
  }
  m
}))

batters_park_factor <- merge(batters %>% filter(yearID >= 1901 & yearID <= 2021), park_factor, 
                             by = c("teamID", "yearID"))


m <- unique(batters_park_factor %>% filter(yearID == 1915) %>% select('teamID'))
## No data for the teams before 1901
## No park factor for them

pre01_batter <- batters %>% filter(yearID <= 1900) 
batters_adj <- rbind(cbind(pre01_batter, park_factor_R = 1, park_factor_H = 1, 
                                   park_factor_HR = 1), batters_park_factor)

batters_adj <- batters_adj %>% mutate(H_PK =  H / park_factor_H) %>% 
  mutate(HR_PK =  HR / park_factor_HR) %>% 
  mutate(R_PK =  R / park_factor_R)

##############
## merge with population
## years to consider
years <- 1871:2021

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/eligible_pop")

MLB_pop <- read.csv('datMLBpop.csv') 

## add weighted populations to data
pops_data <- rbind(
  data.frame(pops = MLB_pop$NL_pop, lgID = "NL", yearID = 1870 + 1:(15*10 + 1)),
  data.frame(pops = MLB_pop$AL_pop, lgID = "AL", yearID = 1870 + 1:(15*10 + 1)))

pop_batters <- merge(batters_adj, pops_data, by = c("yearID", "lgID"))

pop_batters[is.na(pop_batters[,11]), 11] = 0
pop_batters[is.na(pop_batters[,12]), 12] = 0
pop_batters[is.na(pop_batters[,13]), 13] = 0

batters <- do.call(rbind, mclapply(split(pop_batters, 
f = droplevels(as.factor(pop_batters$playerID))), mc.cores = ncores, FUN = function(xx){
  n <- unique(xx$yearID) 
  G <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(G)))
  lgID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(lgID))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(H)))])
  teamID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(teamID))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(H)))])
  AB <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(AB)))
  BB <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(BB)))
  HBP <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(HBP)))
  SH <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(SH)))
  SF <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(SF)))
  hits <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(H_PK)))
  HR <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(HR_PK)))
  obs_hits <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(H)))
  obs_HR <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(HR)))
  pops <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% select(pops))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% select(H)))])
  data_frame(playerID = unique(xx$playerID), yearID = n, lgID = lgID, teamID = teamID, G = G, 
             AB = AB, BB = BB, HBP = HBP, SH = SH, SF = SF, obs_hits = obs_hits, obs_HR = obs_HR, 
             hits = hits, HR = HR, pops = pops)
  }
))

batters <- batters %>% mutate(AVG = hits / AB) %>% mutate(obs_AVG = obs_hits / AB)%>% mutate(HR_AB =  HR / AB)
batters$AVG[is.na(batters$AVG)] <- 0
batters$HR_AB[is.na(batters$HR_AB)] <- 0
batters$obs_AVG[is.na(batters$obs_AVG)] <- 0

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/AVG/raw_data")
write.csv(batters, 'batters_park_factor.csv')



batters_bWAR <- read_csv("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/WAR/raw_data/batters_bWAR.csv")
batters_bWAR$PA <- as.numeric(as.character(batters_bWAR$PA))
batters_bWAR$PA[is.na(batters_bWAR$PA)] <- 0
batters_PA <- do.call(rbind, mclapply(split(batters_bWAR, f = droplevels(as.factor(batters_bWAR$player_ID))), 
                                      mc.cores = ncores, FUN = function(xx){
                                        n <- unique(xx$year_ID)
                                        PA <- sapply(n, function(yy) sum(xx %>% filter(year_ID == yy) %>% select(PA)))
                                        tibble(playerID = unique(xx$player_ID), yearID = n, PA = PA)
                                      } 
)) 

m <- merge(batters_PA, batters, by = c('yearID', 'playerID'))

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/AVG/raw_data")
write.csv(m, 'batters_park_factor.csv')





