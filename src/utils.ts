import _ from 'the-lodash';

export function massageParams(params? : any[]) : any[]
{
    if (!params) {
        params = []
    } else {
        params = params.map(x => {
            if (_.isUndefined(x)) {
                return null;
            }
            if (_.isPlainObject(x) || _.isArray(x)) {
                return _.stableStringify(x);
            }
            return x;
        })
    }
    return params;
}