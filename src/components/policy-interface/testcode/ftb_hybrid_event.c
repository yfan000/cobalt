#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <time.h>
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

	memset(&cinfo, 0, sizeof(cinfo));
	strcpy(cinfo.event_space, "FTB.COBALT.DEMO");
	strcpy(cinfo.client_schema_ver, "0.5");
	strcpy(cinfo.client_name, "Hybrid-Gen");
	strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_POLLING");

	/* Connect to FTB using FTB_Connect */
	ret = FTB_Connect(&cinfo, &handle);
	if (ret != FTB_SUCCESS) {
		printf("FTB_Connect is not successful ret=%d\n", ret);
		exit(-1);
	}

	FTB_event_info_t event_info[1] = {{"HYBRID_FAIL", "INFO"}};
	ret = FTB_Declare_publishable_events(handle, 0, event_info, 1);
	if (ret != FTB_SUCCESS) {
		printf("FTB_Declare_Publishable_events is not successful ret=%d\n", ret);
		exit(-1);
	}

	srandom(time(NULL));
	while (1) {
		ret = 0;
		FTB_event_properties_t *property;
		FTB_event_handle_t ehandle;

		char pl[256] = "PARTITION=ANL-R00-1024;BASH=/bin/bash";


		property = (FTB_event_properties_t *) malloc(sizeof(FTB_event_properties_t));
		property->event_type = 1;
		strcpy(property->event_payload, pl);

		ret = FTB_Publish(handle, "HYBRID_FAIL", property, &ehandle);
		if (ret != FTB_SUCCESS) {
			printf("FTB_Publish failed: %d\n", ret);
			exit(-1);
		}

		const int interval = random() % 10;
		printf("HYBRID_FAIL, Sleeping %d msecs\n", interval * 1000);
		sleep(interval);

	}

	FTB_Disconnect(handle);
	return 0;
}
