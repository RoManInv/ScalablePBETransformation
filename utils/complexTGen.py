import pandas as pd
import os

class complexTGenerator():
    def __init__(self, numrows, probe = None) -> None:
        self.numrows = numrows
        self.probe = probe

    def __readcsv__(self, path, file, header = None) -> pd.DataFrame:
        data = pd.read_csv(os.path.join(path, file), header = header)

        return data
    
    def generate(self, path, file, rename = None, header = False) -> pd.DataFrame:
        data = self.__readcsv__(path, file, header)

        if(header):
            headerRow = data.columns.tolist()
        
        data = data.sample(frac = 1).reset_index(drop = True)

        for i in range(0, len(data.index), self.numrows):
            for j in range(len(data.columns)):
                if(self.probe):
                    data.iloc[i, j] = self.probe + " " + str(data.iloc[i, j])
                for n in range(1, self.numrows):
                    if(i + n < len(data.index)):
                        if(self.probe):
                            data.iloc[i, j] = data.iloc[i, j] + " " + self.probe + " " + str(data.iloc[i+n, j])
                        else:
                            data.iloc[i, j] = data.iloc[i, j] + " " + str(data.iloc[i+n, j])
                    else:
                        break
                # if(i + 1 > len(data.index)):
                #     data.iloc[i, j] = str(data.iloc[i, j])
                # elif(i + 2 > len(data.index)):
                #     data.iloc[i, j] = str(data.iloc[i, j]) + " " + str(data.iloc[i+1, j])
                # else:
                #     data.iloc[i, j] = str(data.iloc[i, j]) + " " + str(data.iloc[i+1, j]) + " " + str(data.iloc[i+2, j])
        data = data[data.index % self.numrows == 0]

        print(data)
        
        if(rename):
            filename = rename
        else:
            if(self.probe):
                filename = file[:-4] + "_complex_probe.csv"
            else:
                filename = file[:-4] + "_complex.csv"
        data.to_csv(os.path.join(path, filename), header = header)

        return data
        