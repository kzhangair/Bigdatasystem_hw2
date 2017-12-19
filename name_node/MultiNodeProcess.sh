#!/bin/bash
./MultiNodeProcessTh01.sh & 
ssh thumm02 "./MultiNodeProcessTh02.sh" &
ssh thumm03 "./MultiNodeProcessTh03.sh" & 
wait
scp thumm02:~/MultiNodeResult_Thumm02 ~/MultiNodeResult_Thumm02
scp thumm03:~/MultiNodeResult_Thumm03 ~/MultiNodeResult_Thumm03
cat MultiNodeResult_Thumm01 MultiNodeResult_Thumm02 MultiNodeResult_Thumm03 > MultiNodeResult
awk 'BEGIN{min = 200000;max = -1;} {if($1<min){min = $1}if($2>max){max = $2}} END{printf("%d %d\n", min, max)}' MultiNodeResult
