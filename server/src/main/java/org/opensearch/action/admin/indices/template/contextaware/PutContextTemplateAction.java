/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.template.contextaware;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

public class PutContextTemplateAction extends ActionType<AcknowledgedResponse> {


    public static final PutContextTemplateAction INSTANCE = new PutContextTemplateAction();
    public static final String NAME = "indices:admin/context_template/put";

    private PutContextTemplateAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
