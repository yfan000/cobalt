/**********************************************************************************/
/* This file is part of FTB (Fault Tolerance Backplance) - the core of CIFTS
 * (Co-ordinated Infrastructure for Fault Tolerant Systems)
 * 
 * See http://www.mcs.anl.gov/research/cifts for more information.
 * 	
 */
/* This software is licensed under BSD. See the file FTB/misc/license.BSD for
 * complete details on your rights to copy, modify, and use this software.
 */
/*********************************************************************************/

/*
 * This file demonstrates a watchdog code that is FTB-enabled.
 *
 * Usage: ./ftb_watchdog
 *
 * Description :A watchdog publishes and polls for its event
 * periodically. The watchdog can be used to ensure that the
 * FTB is up and running.
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include "libftb.h"

static volatile int done = 0;

void Int_handler(int sig)
{
    if (sig == SIGINT)
        done = 1;
}

int main(int argc, char *argv[])
{
    FTB_client_t cinfo;
    FTB_client_handle_t handle;
    FTB_subscribe_handle_t shandle;
    int ret = 0;

    if (argc > 1) {
        if (strcasecmp(argv[1], "usage") == 0) {
            printf("Usage: ./ftb_watchdog");
            exit(0);
        }
    }
    /* Specify the client information needed by the FTB_Connect */
	memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.watchdog");
    strcpy(cinfo.client_schema_ver, "0.5");
    strcpy(cinfo.client_name, "trial-watchdog");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_POLLING");

    /* Connect to FTB using FTB_Connect */
    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect is not successful ret=%d\n", ret);
        exit(-1);
    }

    /* The schema is present in the watchog_schema file. The watchdog schema
     * file is originally present in the  trunk/components/examples directory
     */
    ret = FTB_Declare_publishable_events(handle, NULL, NULL, 0);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_Publishable_events is not successful ret=%d\n", ret);
        exit(-1);
    }

    /*
     * The below section would declare publishable events within the code
     * itself. It could have been used instead of the watchdog_schema.ftb
     * file method
	 */
	/*
     FTB_event_info_t event_info[1] = { {"WATCH_DOG_EVENT", "INFO"} };
     ret = FTB_Declare_publishable_events(handle, 0, event_info, 1);
     if (ret != FTB_SUCCESS) {
     printf("FTB_Declare_publishable_events failed ret=%d!\n", ret); exit(-1);
     }
	 */
    
    /*
     * Subscribe to catch all events in the namespace with region=FTB,
     * component category=all and component name=watchdog
     */
    ret = FTB_Subscribe(&shandle, handle, "event_space=ftb.all.watchdog", NULL, NULL);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed ret=%d!\n", ret);
        exit(-1);
    }

    signal(SIGINT, Int_handler);

    while (1) {
        ret = 0;
        FTB_receive_event_t caught_event;
        FTB_event_handle_t ehandle;

        /*
         * Publish the event with the name = WATCH_DOG_EVENT. This should have
         * been declared using the Declare_publishable_events routine
         */
        ret = FTB_Publish(handle, "WATCH_DOG_EVENT", NULL, &ehandle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Publish failed\n");
            exit(-1);
        }

        sleep(1);

        /* Get the event from the poll queue */
        ret = FTB_Poll_event(shandle, &caught_event);
        if (ret != FTB_SUCCESS) {
            fprintf(stderr, "Watchdog: No event caught Error code is %d!\n", ret);
            break;
        }
        fprintf
            (stderr,
             "Received event details: Event space=%s, Severity=%s, Event name=%s, Client name=%s, Hostname=%s, Seqnum=%d\n",
             caught_event.event_space, caught_event.severity, caught_event.event_name,
             caught_event.client_name, caught_event.incoming_src.hostname, caught_event.seqnum);
        if (done)
            break;
    }
    FTB_Disconnect(handle);
    return 0;
}
