/* GPL 2.1 © Elte 2011 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <dirent.h>

#define VERSION         "0.1"
#define SIGNATUR        "MPLS0100"

typedef struct {
    unsigned char decade, month, day, hour, minute, second;
} MTS_date;

int main (int argc, char *argv[])
{
    struct dirent  *dp = NULL;
    DIR       *dir_ptr = NULL;
    FILE *mpl_file_ptr = NULL;

    int ret = 0;

    unsigned char signatur[] = SIGNATUR, buffer[sizeof(SIGNATUR)];

    if (argc != 2) {
        fprintf(stderr, "Version: %s\n", VERSION);
        fprintf(stderr, "Usage: %s <path to folder containing *.MPL files\n", argv[0]);
        goto ERROR;
    }

    if (chdir(argv[1]) == -1) {
    fprintf(stderr, "Cannot change directory: %s %s\n", argv[1], strerror(errno));
    goto ERROR;
    }

    if ((dir_ptr = opendir (".")) == NULL) {
        fprintf (stderr, "Error reading directory: %s %s\n", argv[1], strerror(errno));
        goto ERROR;
    }

//    printf (" STREAM       DATE       TIME\n");
//    printf ("---------  ----------  --------\n");

    while ((dp = readdir(dir_ptr)) != NULL) {

        if (!strncmp ((dp->d_name) + (strlen (dp->d_name) - 4), ".MPL", 4)) {

            unsigned char file_number[2] = {0, 0},
                                num_desc = 0;
            MTS_date                this = {0, 0, 0, 0, 0, 0};

            if ((mpl_file_ptr = fopen (dp->d_name, "r")) == NULL) {
                fprintf (stderr, "Error opening file: %s %s\n", dp->d_name, strerror(errno));
                goto NEXT_FILE;
            }

            if ((fread (buffer, sizeof(SIGNATUR), 1, mpl_file_ptr) != 1) ||
                (strncmp (buffer, SIGNATUR, sizeof(SIGNATUR)))) {
                fprintf (stderr, "Could not read file signatur. Wrong filetype?\n");
                goto NEXT_FILE;
            }

            // find out how many mts files are described
            // the 66th byte contains this number
            fseek (mpl_file_ptr, 66 - sizeof(SIGNATUR) - 1, SEEK_CUR);
            ret = fgetc(mpl_file_ptr);
            if (ret != EOF) {
                num_desc = (unsigned char) ret;
            } else {
                fprintf (stderr, "Could not read contents\n");
                goto NEXT_FILE;
            }

            // jump to the first occurance of a time stamp and print it
            // iterate till all num_desc time stamps are shown
            // trailer = 50 bytes
            // mts description = 66 bytes
            // actual info starts at 9th byte of mts description
            fseek (mpl_file_ptr, -50 - 66*num_desc - 48 +2, SEEK_END);

            while (num_desc > 0) {

                int i;
                const unsigned char time_stamp_sig[] = {1, 3, 5, 0x01, 0, 0, 0, 2};

                num_desc--;
                fseek (mpl_file_ptr, 48, SEEK_CUR);

                // scan for time stamp signatur
                for (i=0; i<8; i++) {
                    if ((ret = fgetc(mpl_file_ptr)) != time_stamp_sig[i]) {
                        fprintf (stderr, "Could not parse contents\n");
                        goto NEXT_FILE;
                    }
                }

                // scan time stamp
                if ((ret = fscanf (mpl_file_ptr, "%c%c\x1E%*1[ ]%c%c%c%c%c%c",
                              &file_number[0], &file_number[1],
                              &this.decade, &this.month, &this.day,
                              &this.hour, &this.minute, &this.second)) != 8) {
                    fprintf (stderr, "Could not parse time stamp\n");
                    goto NEXT_FILE;
                }
                printf ("%.5d.MTS  20%.2X/%.2X/%.2X  %.2X:%.2X:%.2X\n",
                        (file_number[0]<<8) + file_number[1],
                        this.decade, this.month, this.day,
                        this.hour, this.minute, this.second);
            }
NEXT_FILE:
            fclose (mpl_file_ptr);
        }
    }
ERROR:
    closedir (dir_ptr);
}
