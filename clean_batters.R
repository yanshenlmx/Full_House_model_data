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
setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season")
people_bb_f <- read.csv("people_bb_f.csv") 
people_bb_f <- people_bb_f %>% select(key_retro, key_bbref, key_fangraphs, name_last, name_first)
people_bb_f <- people_bb_f[complete.cases(people_bb_f), ]

people_bb_f <- people_bb_f[people_bb_f$key_retro != "",]
people_bb_f$key_bbref <- as.character(people_bb_f$key_bbref)
people_bb_f[people_bb_f$key_bbref == "",]$key_bbref <- c("adonjo01", "bazsh01", "castier01", "colemdy01", 
                                                         "contrro01", "crousha01", "cruzon01", "diazjh01", 
                                                         "dohyky01", "forteni01", "friaslu01", "friedtj01", 
                                                         "giambtr01", "aaa", "heasljo01", "henrypa01", 
                                                         "leedy01", "martise01", "moretda01", "obrieri01", 
                                                         "paynety01", "romerjh01", "sanmare01", "stridsp01", 
                                                         "viladry01", "zerpaan01")
people_bb_f <- people_bb_f[people_bb_f$key_bbref != "aaa",]

people_park_bb <- read.csv('people_park_bb.csv')
people_park_bb <- people_park_bb %>% select(playerID, bbrefID)
people_park_bb$playerID <- as.character(people_park_bb$playerID)
people_park_bb$bbrefID <- as.character(people_park_bb$bbrefID)
people_park_bb[people_park_bb$bbrefID =="",]$bbrefID <- people_park_bb[people_park_bb$bbrefID =="",]$playerID
setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season")
write.csv(people_bb_f, 'people_bb_f.csv')
write.csv(people_park_bb, 'people_park_bb.csv')

batters_combined_b <- read.csv("batters/WAR/raw_data/batters_combined_b.csv",stringsAsFactors = FALSE)
batters_combined_f <- read.csv("batters/WAR/raw_data/batters_combined_f.csv",stringsAsFactors = FALSE)
batters_park_factor <- read.csv("batters/AVG/raw_data/batters_park_factor.csv",stringsAsFactors = FALSE)

batters_all_park <- do.call(rbind, mclapply(people_park_bb$playerID, mc.cores = ncores, FUN = function(xx){
  bb_ID <- unique(people_park_bb$bbrefID[people_park_bb$playerID == xx])
  f_ID <- unique(people_bb_f$key_fangraphs[people_bb_f$key_bbref == as.character(bb_ID)])
  
  if (length(bb_ID) != 0 && length(f_ID) != 0) {
    dat_AVG <- batters_park_factor %>% filter(playerID == xx) %>% 
      select(yearID, AB, BB, HBP, SH, SF, hits, HR, AVG, HR_AB, pops)
    dat_bWAR <- batters_combined_b %>% filter(playerID == bb_ID) %>%
      mutate(bWAR = WAR) %>% 
      select(playerID, yearID, name, G, PA, bWAR) 
    dat_fWAR <- batters_combined_f %>% filter(playerID == f_ID) %>% mutate(fWAR = WAR)%>%
      select(yearID, fWAR) 
    
    m1 <- merge(dat_AVG, dat_bWAR, by = "yearID")
    m2 <- merge(m1, dat_fWAR, by = "yearID")
    m2 %>% select(playerID, name, yearID, G, AB, PA, hits, AVG, HR, HR_AB, BB, HBP, SH, SF, bWAR, fWAR, pops)
  }
  }))

batters_all_bb <- do.call(rbind, mclapply(people_park_bb$bbrefID, mc.cores = ncores, FUN = function(xx){
  park_ID <- unique(people_park_bb$playerID[people_park_bb$bbrefID == xx])
  f_ID <- unique(people_bb_f$key_fangraphs[people_bb_f$key_bbref == as.character(xx)])
  
  if (length(park_ID) != 0 && length(f_ID) != 0) {
    dat_AVG <- batters_park_factor %>% filter(playerID == park_ID) %>% 
      select(yearID, AB, BB, HBP, SH, SF, hits, HR, AVG, HR_AB, pops)
    dat_bWAR <- batters_combined_b %>% filter(playerID == xx) %>%
      mutate(bWAR = WAR) %>% 
      select(playerID, yearID, name, G, PA, bWAR) 
    dat_fWAR <- batters_combined_f %>% filter(playerID == f_ID) %>% mutate(fWAR = WAR)%>%
      select(yearID, fWAR) 
    
    m1 <- merge(dat_AVG, dat_bWAR, by = "yearID")
    m2 <- merge(m1, dat_fWAR, by = "yearID")
    m2 %>% select(playerID, name, yearID, G, AB, PA, hits, AVG, HR, HR_AB, BB, HBP, SH, SF, bWAR, fWAR, pops)
  }
}))

batters_all_f <- do.call(rbind, mclapply(people_bb_f$key_fangraphs, mc.cores = ncores, FUN = function(xx){
  bb_ID <- unique(people_bb_f$key_bbref[people_bb_f$key_fangraphs == xx])
  park_ID <- unique(people_park_bb$playerID[people_park_bb$bbrefID == as.character(bb_ID)])
  
  if (length(park_ID) != 0 && length(bb_ID) != 0) {
    dat_AVG <- batters_park_factor %>% filter(playerID == park_ID) %>% 
      select(yearID, AB, BB, HBP, SH, SF, hits, HR, AVG, HR_AB, pops)
    dat_bWAR <- batters_combined_b %>% filter(playerID == bb_ID) %>%
      mutate(bWAR = WAR) %>% 
      select(playerID, yearID, name, G, PA, bWAR) 
    dat_fWAR <- batters_combined_f %>% filter(playerID == xx) %>% mutate(fWAR = WAR)%>%
      select(yearID, fWAR) 
    
    m1 <- merge(dat_AVG, dat_bWAR, by = "yearID")
    m2 <- merge(m1, dat_fWAR, by = "yearID")
    m2 %>% select(playerID, name, yearID, G, AB, PA, hits, AVG, HR, HR_AB, BB, HBP, SH, SF, bWAR, fWAR, pops)
  }
}))

batters_all <- do.call(rbind, mclapply(people_park_bb$playerID, mc.cores = ncores, FUN = function(xx){
  bb_ID <- unique(people_park_bb$bbrefID[people_park_bb$playerID == xx])
  f_ID <- unique(people_bb_f$key_fangraphs[people_bb_f$key_bbref == as.character(bb_ID)])
  
  if (length(bb_ID) != 0 && length(f_ID) != 0) {
    dat_AVG <- batters_park_factor %>% filter(playerID == xx) %>%
      mutate(chadID = playerID) %>% 
      select(chadID, yearID, AB, BB, HBP, SH, SF, obs_hits, hits, obs_HR, HR, obs_AVG, AVG, HR_AB, pops)
    dat_bWAR <- batters_combined_b %>% filter(playerID == bb_ID) %>% 
      mutate(bbID = playerID) %>%
      mutate(bWAR = WAR) %>% 
      select(bbID, age, yearID, name, G, PA, bWAR) 
    dat_fWAR <- batters_combined_f %>% filter(playerID == f_ID) %>% mutate(fWAR = WAR) %>% 
      mutate(fID = playerID) %>% 
      select(yearID, fID, fWAR) 
    
    m1 <- merge(dat_AVG, dat_bWAR, by = "yearID")
    m2 <- merge(m1, dat_fWAR, by = "yearID")
    m2 %>% select(chadID, bbID, fID, name, yearID, age, G, AB, PA, obs_hits, hits, 
                  obs_AVG, AVG, obs_HR, HR, HR_AB, BB, HBP, SH, SF, bWAR, fWAR, pops)
  }
}))

check_park <- do.call(rbind, mclapply(batters_park_factor$playerID, mc.cores = ncores, FUN = function(xx){
  if (! xx %in% batters_all$chadID) {
    c(as.character(xx), nrow(batters_park_factor %>% filter(playerID == xx)))
  }
}))

## check the players are not in the dataset. 
check_park <- as.data.frame(check_park)
check_park$V2 <- as.numeric(as.character(check_park$V2))
check_park <- check_park[!duplicated(check_park),]

setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters")
write.csv(batters_all, 'batters_all.csv')









## willsfr01: 9 seasons as a hitter but career AB is 0. 
## name structure is different between batter park factor and bWAR
## Erisbel Arruebarrena: in BBref it is arrueba01
## name structure from BBref has "'", ".", and "_"
######## 
## change the names from bWAR to chadwick
## WAR is empty or last two digits are different. 

batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "alexasc02", 
                                       "alexasc01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == 'davieza02', 
                                       'davieza01' )
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == 'harriwi02', 
                                       'harriwi01' )
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == 'harriwi10', 
                                       'harriwi02' )
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == 'lintz02', 
                                       'lintz01' )
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == 'montafr02', 
                                       'montafr01' )
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == 'willima10', 
                                       'willima07' )

batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "armstsa01", 
                                       "armstbo01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "baldwo.01", 
                                       "baldwof01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "beeksja02", 
                                       "beeksja01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "brownro02", 
                                       "brownro01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "callaji01", 
                                       "callajo02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "callaji02", 
                                       "callaji01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "castiru02", 
                                       "castiru01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "clarkbi01", 
                                       "clarkwi03")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "clarkbi02", 
                                       "clarkbi01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "campbma02", 
                                       "campbhu02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "campbma01", 
                                       "campbmi02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "campovi01", 
                                       "campojo01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "fitzgjo04", 
                                       "fitzgjo03")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "furcara02", 
                                       "furcara01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "garcifr03", 
                                       "garcifr02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "greenri02", 
                                       "greenri01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "gillejo01", 
                                       "gilledu01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "gonzaga02", 
                                       "gonzaga01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "graydo02", 
                                       "grayst01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "graysa01", 
                                       "graydo02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "hansff.01", 
                                       "hansffc01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "hoffre01", 
                                       "hoffch01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "jacquth01", 
                                       "jacquto01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "jonesja05", 
                                       "jonesja")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "jonesja04", 
                                       "jonesja05")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "jonesja", 
                                       "jonesja04")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "lockewa01", 
                                       "lockean01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "lopezro02", 
                                       "lopezro01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "murraco02", 
                                       "murraco01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "newelt.01", 
                                       "newelte01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "noelri02", 
                                       "noelri01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'brida02", 
                                       "obrieda02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'conbr02", 
                                       "oconnbr01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'donjo02", 
                                       "odonojo01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'hagha01", 
                                       "ohageha01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'meato01", 
                                       "omearto01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'nea01", 
                                       "oneal01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'neibi01", 
                                       "oneilbi01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'neij.01", 
                                       "oneilj01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "obriepe01", 
                                       "obriepe04")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "o'bripe01", 
                                       "obriepe01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "ortizjo04", 
                                       "ortizjo02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "ortizra02", 
                                       "ortizra01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "ramirju02", 
                                       "ramirju01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "reardji01", 
                                       "reardje02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "rodrijo04", 
                                       "rodrijo02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "rodrijo06", 
                                       "rodrijo04")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "santajo02", 
                                       "santajo01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "schrest02", 
                                       "schrest01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "smithch07", 
                                       "smithch06")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "smithch08", 
                                       "smithch07")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "smithda07", 
                                       "smithda06")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "stricsc02", 
                                       "stricsc01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "sweenje02", 
                                       "sweened01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "taylobe10", 
                                       "taylobe03")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "taylobe11", 
                                       "taylobe04")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "walkeke02", 
                                       "walkeke01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "watsoma03", 
                                       "watsoma01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "willije01", 
                                       "willije")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "willije02", 
                                       "willije01")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "willije", 
                                       "willije02")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "willima09", 
                                       "willima06")
batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                       x == "willima11", 
                                       "willima08")



m <- intersect(batters_park_factor %>% select(c('playerID', 'yearID')), 
               batters_combined_b %>% select(c('playerID', 'yearID')), 
               by = c('playerID', 'yearID'))
m_bWAR <- setdiff(batters_combined_b %>% select(c('playerID', 'yearID')), m)
BBB <- do.call(rbind, mclapply(unique(m_bWAR$playerID), mc.cores = ncores, FUN = function(xx){
  num <- str_sub(xx, -2, -1)
  t <- unique(batters_combined_b %>% filter(playerID == xx) %>% select(name))
  t <- gsub("'", "", as.character(t$name))
  t <- gsub("\\.", "", t)
  t <- gsub("_", "", t)
  tt <- strsplit(as.character(tolower(t)), " ")
  if (xx == "callaji01") {
    newID <- "callaji01"
  } else if (length(tt[[1]]) == 1) {
    newID <- paste(str_sub(tt[[1]][1], 1, 5), num, sep = "")
  } else if (length(tt[[1]]) == 2) {
    newID <- paste(str_sub(tt[[1]][2], 1, 5), str_sub(tt[[1]][1], 1, 2), num, sep = "")
  } else if (length(tt[[1]]) == 3) {
    if (nchar(tt[[1]][1]) == 1) {
      newID <- paste(str_sub(tt[[1]][3], 1, 5), str_sub(tt[[1]][1], 1, 1), str_sub(tt[[1]][2], 1, 1), num, sep = "")
    } else {
      ttt <- paste(tt[[1]][2], tt[[1]][3], sep = "")
      newID <- paste(str_sub(ttt, 1, 5), str_sub(tt[[1]][1], 1, 2), num, sep = "")
    }
  } else if (length(tt[[1]]) == 4){
    ttt <- paste(tt[[1]][2], tt[[1]][3],tt[[1]][4], sep = "")
    newID <- paste(str_sub(ttt, 1, 5), str_sub(tt[[1]][1], 1, 2), num, sep = "")
  }
  cbind(xx,m_bWAR %>% filter(playerID == xx) %>% mutate(playerID = newID))
}
)) 

mm <- intersect(batters_park_factor %>% select(c('playerID', 'yearID')), 
               BBB %>% select(c('playerID', 'yearID')), 
               by = c('playerID', 'yearID'))

for (i in 1:nrow(BBB)) {
  batters_combined_b$playerID <- replace(x <- batters_combined_b$playerID, 
                                         x == BBB$xx[i], BBB$playerID[i])
}

m <- intersect(batters_park_factor %>% select(c('playerID', 'yearID')), 
               batters_combined_b %>% select(c('playerID', 'yearID')), 
               by = c('playerID', 'yearID'))

BPF_bWAR <- merge(batters_combined_b %>% 
                    select(c('playerID', 'yearID', 'name', 'G', 'PA', 'WAR', 'pops')), 
                  batters_park_factor %>% 
                    select(c('playerID', 'yearID', 'AB', 'BB', 'HBP', 'SH', 'SF', 'hits', 'HR')),
                  by = c('playerID', 'yearID'))

## players from fWAR does not have names

DDD <- do.call(rbind, mclapply(unique(batters_combined_f$playerID), mc.cores = ncores, FUN = function(xx){
  t <- unique(batters_combined_f %>% filter(playerID == xx) %>% select(name))
  
  t <- gsub("'", "", as.character(t$name))
  t <- gsub("\\.", "", t)
  t <- gsub("_", "", t)
  t <- gsub("-", "", t)
  t <- gsub("\\([^()]*)", "", t)
  tt <- strsplit(as.character(tolower(t)), " ")
  if (length(tt[[1]]) == 4){
    ttt <- paste(tt[[1]][2], tt[[1]][3],tt[[1]][4], sep = "")
    newID <- paste(str_sub(ttt, 1, 5), str_sub(tt[[1]][1], 1, 2), sep = "")
  } else if (length(tt[[1]]) == 3){
    if (tt[[1]][3] == "jr") {
      ttt <- paste(tt[[1]][2], tt[[1]][3], sep = "")
      newID <- paste(str_sub(ttt, 1, 5), str_sub(tt[[1]][1], 1, 2), sep = "")
    } else if (tt[[1]][2] == "van") {
      ttt <- paste(tt[[1]][2], tt[[1]][3], sep = "")
      newID <- paste(str_sub(ttt, 1, 5), str_sub(tt[[1]][1], 1, 2), sep = "")
    } else if (tt[[1]][2] == "vander") {
      ttt <- paste(tt[[1]][2], tt[[1]][3], sep = "")
      newID <- paste(str_sub(ttt, 1, 5), str_sub(tt[[1]][1], 1, 2), sep = "")
    } else if (tt[[1]][2] == "de") {
      ttt <- paste(tt[[1]][2], tt[[1]][3], sep = "")
      newID <- paste(str_sub(ttt, 1, 5), str_sub(tt[[1]][1], 1, 2), sep = "")
    } else {
      ttt <- paste(tt[[1]][1], tt[[1]][2], sep = "")
      newID <- paste(str_sub(tt[[1]][3], 1, 5), str_sub(ttt, 1, 2), sep = "")
      }
    } else if (length(tt[[1]]) == 2){
    newID <- paste(str_sub(tt[[1]][2], 1, 5), str_sub(tt[[1]][1], 1, 2), sep = "")
  } 
  cbind(newID, batters_combined_f %>% filter(playerID == xx))
}
)) 

EEE <- do.call(rbind, mclapply(unique(DDD$newID), mc.cores = ncores, FUN = function(xx){
  t <- unique(DDD %>% filter(newID == xx) %>% select(playerID))
  n <- nrow(t)
  if (n == 1) {
    DDD %>% filter(newID == xx) %>% mutate(newID = paste(newID, '01', sep = ""))
  } else {
    miny <- as.data.frame(do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
      c(xx, yy, min(DDD %>% filter(playerID == yy) %>% select(yearID)))
    })))
    if (xx == "jonesja") {
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')+1) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } else if (xx == "mccarjo") {
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')+1) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } else if (xx == "youngch") {
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')+1) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } else if (xx == "woodjo") {
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')+1) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } else if (xx == "milleed") {
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')+1) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } else if (xx == "millebo") {
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')+1) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } else if (xx == "smithbo") {
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')+1) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } else if (xx == "smithja") {
      ra <- rank(miny$V3, ties.method = 'first')+1
      ra[ra == 2] <- 1
      miny <- miny %>% 
        mutate(rank = ra) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } 
    else if (xx == "schuljo") {
      ra <- rank(miny$V3, ties.method = 'first')+1
      ra[ra == 2] <- 1
      miny <- miny %>% 
        mutate(rank = ra) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } 
    else if (xx == "colemjo") {
      ra <- rank(miny$V3, ties.method = 'first')+1
      ra[ra == 2] <- 1
      miny <- miny %>% 
        mutate(rank = ra) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } 
    else if (xx == "morrijo") {
      ra <- rank(miny$V3, ties.method = 'first')+1
      ra[ra == 2] <- 1
      ra[ra == 3] <- 2
      miny <- miny %>% 
        mutate(rank = ra) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    } 
    else{
      miny <- miny %>% 
        mutate(rank = rank(V3, ties.method = 'first')) %>% 
        mutate(newID = paste(xx, '0', rank, sep = ""))
      do.call(rbind, mclapply(t$playerID, mc.cores = ncores, FUN = function(yy){
        DDD %>% filter(playerID == yy) %>% mutate(newID = miny$newID[miny$V2 == yy])
      }))
    }
  }
}
)) 

colnames(EEE)[c(1,3)] <- c('playerID', 'fWAR_ID')

EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "willima06", 
                        "willima08")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "willima05", 
                        "willima07")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "willima04", 
                        "willima06")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "willima03", 
                        "willima04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "jonesch05", 
                        "jonesch06")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "jonesch04", 
                        "jonesch05")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "jonesch03", 
                        "jonesch04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "jonesch02", 
                        "jonesch03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "brownke01", 
                        "brownke")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "brownke02", 
                        "brownke01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "brownke", 
                        "brownke02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "brownke03", 
                        "brownke04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "burkejo03", 
                        "burkejo04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "burkejo02", 
                        "burkejo03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "burkejo01", 
                        "burkejo02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "valenjo03", 
                        "valenjo04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "valenjo01", 
                        "valenjo03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "hernaro02", 
                        "carmofa01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "roberda09", 
                        "roberda10")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "roberda08", 
                        "roberda09")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "roberda07", 
                        "roberda08")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "roberda06", 
                        "roberda07")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "roberda05", 
                        "roberda06")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "roberda04", 
                        "roberda05")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "milletr01", 
                        "milletr")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "milletr02", 
                        "milletr01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "milletr", 
                        "milletr02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "martial02", 
                        "martial03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "hoparch01", 
                        "parkch01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "powelja03", 
                        "powelja04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "powelja02", 
                        "powelja03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "robinfr01", 
                        "robinfr02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "cardejo01", 
                        "cardejo02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "smithre04", 
                        "smithre06")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "smithre03", 
                        "smithre04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "smithre02", 
                        "smithre03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "smithre01", 
                        "smithre02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "tennefr01", 
                        "tennefr02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "robinbi01", 
                        "robinbi02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "garcija03", 
                        "garcija04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "garcija02", 
                        "garcija03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "garcija01", 
                        "garcija02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "casilsa01", 
                        "garcija01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "reedje02", 
                        "reedje03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "reedje01", 
                        "reedje02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "hernafe02", 
                        "hernafe03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "hernafe01", 
                        "hernafe02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "clarkwi03", 
                        "clarkwi")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "clarkwi02", 
                        "clarkwi03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "clarkwi", 
                        "clarkwi02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "hansero01", 
                        "hansero02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "walkege01", 
                        "walkege02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "baileho01", 
                        "baileho02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "abernte01", 
                        "abernte02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "sullibi03", 
                        "sullibi04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "sullibi02", 
                        "sullibi03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "whitebi02", 
                        "whitebi")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "whitebi03", 
                        "whitebi02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "whitebi", 
                        "whitebi03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "zimmejo01", 
                        "zimmejo02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "murphjo04", 
                        "murphjr01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "murphjo03", 
                        "murphjo04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "murphjo02", 
                        "murphjo03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "murphjo01", 
                        "murphjo02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "allenjo01", 
                        "allenjo02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "rogerke02", 
                        "rogerke")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "rogerke01", 
                        "rogerke02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "rogerke", 
                        "rogerke01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "stantgi01", 
                        "stantmi03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "uptonme01", 
                        "uptonbj01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "anderla02", 
                        "anderla03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "anderla01", 
                        "anderla02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "dineebi01", 
                        "dinnebi01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "mcdermi02", 
                        "mcdermi03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "smithal03", 
                        "smithal04")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "smithal02", 
                        "smithal03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "ducapa01", 
                        "loducpa01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "bakerje02", 
                        "bakerje03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "nunezed01", 
                        "nunezed02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "blackch01", 
                        "blackch02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "berrych01", 
                        "berrych02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "brownto04", 
                        "brownto05")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "howelro01", 
                        "howelro02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "rodriri03", 
                        "rodriri05")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "rodriri02", 
                        "rodriri03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "rodriri01", 
                        "rodriri02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "youkike01", 
                        "youklke01")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "johnsda04", 
                        "johnsda07")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "johnsda03", 
                        "johnsda06")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "chambjo02", 
                        "chambjo03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "wilsoju01", 
                        "wilsoju10")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "murphda07", 
                        "murphda08")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "murphda06", 
                        "murphda07")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "snydech01", 
                        "snydech02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "bakersc01", 
                        "bakersc02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "butlebi02", 
                        "butlebi03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "butlebi01", 
                        "butlebi02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "gardnbi01", 
                        "gardnbi02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "housefr02", 
                        "housefr03")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "housefr01", 
                        "housefr02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "longda01", 
                        "longda02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "oconnda01", 
                        "oconnda02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "sullimi01", 
                        "sullimi02")
EEE$playerID <- replace(x <- EEE$playerID, 
                        x == "geraled01", 
                        "fitzged01")

m <- intersect(EEE %>% select(c('yearID', 'playerID')), 
                BPF_bWAR %>% select(c('yearID', 'playerID')), 
                by = c('yearID', 'playerID'))

m_fWAR <- setdiff(EEE %>% select(c('playerID', 'yearID')), m)
m_count <- do.call(rbind, mclapply(unique(m_fWAR$playerID), mc.cores = ncores, FUN = function(xx){
  c(xx, nrow(m_fWAR %>% filter(playerID == xx)))
  }
)) 
m_count <- as.data.frame(m_count)
m_count$V2 <- as.numeric(as.character(m_count$V2))
m_count %>% arrange(-V2) %>% filter(V2 >= 10)

colnames(BPF_bWAR)[6] <- 'bWAR'
BPF_bf <- merge(BPF_bWAR, EEE %>% select(playerID, yearID, WAR), by = c('playerID', 'yearID'))
setwd("~/Desktop/PhD_UIUC/ProfEck/baseball/new_season/batters")
write.csv(BPF_bf, 'batters_all.csv')






