import _ from 'the-lodash';
import crypto from 'crypto';

export function calculateObjectHash(obj : any) : Buffer
{
    if (_.isNullOrUndefined(obj)) {
        throw new Error('NO Object');
    }

    const str = _.stableStringify(obj);

    const sha256 = crypto.createHash('sha256');
    sha256.update(str);
    const value = sha256.digest();
    return value;
}

export function calculateObjectHashStr(obj : any) : string
{
    return calculateObjectHash(obj).toString('hex');
}