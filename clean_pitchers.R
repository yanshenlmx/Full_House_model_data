## load in software
rm(list=ls())
library(tidyverse)
library(orderstats)
library(Pareto)
library(parallel)
library(doParallel)
library(readr)
library(EnvStats)
ncores <- detectCores() - 1

########
## park factor for ERA
setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers")
raw_pitcher <- read.csv("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers/Pitching.csv")

raw_pitcher[is.na(raw_pitcher$lgID),]$lgID <- 'NL'
raw_pitcher$lgID[raw_pitcher$lgID == 'AA'] <- 'NL'
raw_pitcher$lgID[raw_pitcher$lgID == 'UA'] <- 'NL'
raw_pitcher$lgID[raw_pitcher$lgID == 'PL'] <- 'NL'
raw_pitcher$lgID[raw_pitcher$lgID == 'FL'] <- 'NL'

pitchers <- raw_pitcher %>% 
  select(c(playerID, yearID, teamID, lgID, IPouts, ER, HR, H, SO, BB, HBP))

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters/AVG/raw_data")
## raw data from Chadwick.
raw_batter <- read.csv("raw_batter.csv")
raw_batter[is.na(raw_batter$lgID),]$lgID <- 'NL'
batters <- raw_batter %>% filter(lgID %in% c('NL', 'AL')) %>% select(-c(stint,X2B, X3B, RBI:CS, SO, IBB, GIDP))

## park factor for Runs, Hits and Home runs.
years <- c(1901:2020)
team_log <- do.call(rbind, lapply(years, function(x) 
  cbind(read.csv(paste("teams-", x, ".csv", sep = "")), yearID = x)))

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
setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers")
pitchers_2021 <- read.csv('pitchers_2021.csv',stringsAsFactors = FALSE) %>%
  filter(! Tm %in% 'TOT') 
colnames(pitchers_2021)[c(3,5,6)] <- c('bbID','teamID', 'lgID')
pitchers_2021$teamID <- gsub('CHW', 'CHA', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('CHC', 'CHN', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('KCR', 'KCA', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('LAD', 'LAN', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('NYY', 'NYA', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('NYM', 'NYN', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('SDP', 'SDN', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('SFG', 'SFN', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('STL', 'SLN', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('TBR', 'TBA', pitchers_2021$teamID)
pitchers_2021$teamID <- gsub('WSN', 'WAS', pitchers_2021$teamID)

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season")
people_park_bb <- read.csv("people_park_bb.csv", stringsAsFactors = FALSE) %>% 
  select(-X)
colnames(people_park_bb)[2] <- 'bbID'

m <- merge(pitchers_2021, people_park_bb, by = "bbID")

pitchers <- rbind(pitchers, m %>% 
                   mutate(yearID = 2021) %>% mutate(IPouts = IP *3) %>% 
                   select('playerID', 'yearID', 'teamID', 'lgID', 
                          'IPouts', 'ER', 'HR', 'H','SO','BB', 'HBP'))

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

pitchers_park_factor <- merge(pitchers %>% filter(yearID >= 1901 & yearID <= 2021), park_factor, 
                             by = c("teamID", "yearID"))


## No data for the teams before 1901
## No park factor for them

pre01_pitcher <- pitchers %>% filter(yearID <= 1900) 
pitchers_adj <- rbind(cbind(pre01_pitcher, park_factor_R = 1, park_factor_H = 1, 
                           park_factor_HR = 1), pitchers_park_factor)

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

pop_pitchers <- merge(pitchers_adj, pops_data, by = c("yearID", "lgID"))

pop_pitchers$HBP[is.na(pop_pitchers$HBP)] <- 0

pitchers <- pop_pitchers %>% group_by(playerID, yearID) %>% 
  summarise(playerID = unique(playerID), yearID = unique(yearID), 
            lgID = lgID[which.max(IPouts)], 
            teamID = teamID[which.max(IPouts)],IPouts = sum(IPouts), 
            obs_ER = sum(ER), obs_HR = sum(HR), obs_H = sum(H), 
            SO = sum(SO), ER_PF = sum(ER / park_factor_R), 
            HR_PF = sum(HR / park_factor_HR), 
            H_PF = sum(H / park_factor_H), BB = sum(BB), HBP = sum(HBP),
            pops = mean(pops))



pitchers <- do.call(rbind, mclapply(split(pop_pitchers, f = droplevels(as.factor(pop_pitchers$playerID))),mc.cores = ncores, FUN = function(xx){
  n <- unique(xx$yearID)
  lgID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% 
                select(lgID))[which.max(as.matrix(xx %>% filter(yearID == yy) %>% 
                                                    select(IPouts)))])
  teamID <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% 
                select(teamID))[which.max(as.matrix(xx %>% filter(yearID == yy) 
                                                    %>% select(IPouts)))])
  IPouts <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(IPouts)))
  SO <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(SO)))
  obs_ER <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(ER)))
  ER <- sapply(n, function(yy) sum(xx %>% filter(yearID == yy) %>% select(ER_PK)))
  pops <- sapply(n, function(yy) as.matrix(xx %>% filter(yearID == yy) %>% 
                select(pops))[which.max(as.matrix(xx %>% filter(yearID == yy) 
                                                  %>% select(IPouts)))])
  data_frame(playerID = unique(xx$playerID), yearID = n, lgID = lgID, teamID = teamID, 
             IPouts = IPouts, SO = SO, obs_ER = obs_ER, ER = ER, pops = pops)
  }
))

pitchers <- pitchers %>% mutate(ERA = ER / IPouts * 27) %>% mutate(obs_ERA = obs_ER / IPouts * 27)
pitchers$ERA[is.na(pitchers$ERA)] <- 0
pitchers$obs_ERA[is.na(pitchers$obs_ERA)] <- 0

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers")
write.csv(pitchers, 'pitchers_park_factor.csv')

########
## merge the datasets from chadwick,BBref and fangraphs. 
setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season")
people_park_bb <- read.csv('people_park_bb.csv', stringsAsFactors = FALSE) %>% select(-X)
people_bb_f <- read.csv('people_bb_f.csv', stringsAsFactors = FALSE) %>% select(-X)

pitchers_park_factor <- read.csv("pitchers/pitchers_park_factor.csv", stringsAsFactors = FALSE) %>% 
  select(-X)
pitchers_combined_b <- read.csv("pitchers/WAR/raw_data/pitchers_combined_b.csv", stringsAsFactors = FALSE) %>% 
  select(-X)
pitchers_combined_f <- read.csv("pitchers/WAR/raw_data/pitchers_combined_f.csv", stringsAsFactors = FALSE) %>% 
  select(-X)

pitchers_all <- do.call(rbind, mclapply(people_park_bb$playerID, mc.cores = ncores, FUN = function(xx){
  bb_ID <- unique(people_park_bb$bbrefID[people_park_bb$playerID == xx])
  f_ID <- unique(people_bb_f$key_fangraphs[people_bb_f$key_bbref == as.character(bb_ID)])
  
  if (length(bb_ID) != 0 && length(f_ID) != 0) {
    dat_ERA <- pitchers_park_factor %>% filter(playerID == xx) %>%
      mutate(chadID = playerID) %>% 
      select(chadID, yearID, SO, BB, HBP, obs_ER, ER_PF, 
             obs_HR, HR_PF, obs_H, H_PF, pops)
    dat_bWAR <- pitchers_combined_b %>% filter(playerID == bb_ID) %>% 
      mutate(bbID = playerID) %>%
      mutate(bWAR = WAR) %>% 
      select(bbID, yearID, age, lgID, teamID, name, bWAR) 
    dat_fWAR <- pitchers_combined_f %>% filter(playerID == f_ID) %>% 
      mutate(fWAR = WAR) %>% 
      mutate(fID = playerID) %>% 
      select(yearID, fID, IP, fWAR, FIP, cFIP) 
    
    m1 <- merge(dat_ERA, dat_bWAR, by = "yearID")
    m2 <- merge(m1, dat_fWAR, by = "yearID")
    m2 %>% select(chadID, bbID, fID, name, age, yearID, lgID, teamID,IP, 
                  SO, BB, HBP, obs_ER, ER_PF, obs_HR, HR_PF, obs_H, H_PF, 
                  FIP, cFIP, bWAR, fWAR, pops)
  }
}))

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/pitchers")
write.csv(pitchers_all, 'pitchers_all.csv')







